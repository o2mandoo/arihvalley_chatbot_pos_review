#!/usr/bin/env python3
"""리뷰 파이프라인 실행 스크립트 (크롤링 중심)."""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

# 모듈 경로 추가
sys.path.append(str(Path(__file__).parent))

from config.config import CRAWLING_CONFIG, STORES, get_analysis_filepath, get_review_filepath
from modules import LLMReviewAnalyzer, NaverReviewCrawler, ReportGenerator, ReviewDataManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def run_store_pipeline(
    store_key: str,
    skip_crawling: bool = False,
    with_analysis: bool = False,
    max_workers: int = 10,
    wait_time: int = CRAWLING_CONFIG["wait_time"],
    click_wait: int = CRAWLING_CONFIG["click_wait"],
    headless: bool = CRAWLING_CONFIG["headless"],
) -> None:
    if store_key not in STORES:
        raise ValueError(f"매장 정보를 찾을 수 없습니다: {store_key}")

    store_info = STORES[store_key]
    store_name = store_info["store_name"]
    review_filepath = get_review_filepath(store_name)
    analysis_filepath = get_analysis_filepath(store_name)

    logger.info("=" * 80)
    logger.info("매장: %s (%s)", store_key, store_name)
    logger.info("크롤링: %s", "건너뜀" if skip_crawling else "실행")
    logger.info("LLM 분석: %s", "실행" if with_analysis else "건너뜀")
    logger.info("=" * 80)

    if not skip_crawling:
        logger.info("[1/2] 리뷰 크롤링 + 증분 병합")
        crawler = NaverReviewCrawler(headless=headless)
        new_reviews = crawler.crawl_reviews(
            url=store_info["url"],
            wait_time=wait_time,
            click_wait=click_wait,
        )
        logger.info("수집 리뷰: %d건", len(new_reviews))

        manager = ReviewDataManager(str(review_filepath))
        manager.load_data()
        before = manager.get_statistics()["total_reviews"]
        added_count = manager.merge_and_update(new_reviews)
        after = manager.get_statistics()["total_reviews"]

        logger.info("기존 리뷰: %d건", before)
        logger.info("신규 추가: %d건", added_count)
        logger.info("누적 리뷰: %d건", after)
    else:
        logger.info("[1/2] 크롤링 단계 건너뜀")

    if not with_analysis:
        logger.info("[2/2] LLM 분석 단계 건너뜀 (크롤링 중심 모드)")
        logger.info("리뷰 데이터 파일: %s", review_filepath)
        return

    logger.info("[2/2] LLM 분석 + 리포트")
    if not review_filepath.exists():
        raise FileNotFoundError(
            f"리뷰 파일이 없어 분석할 수 없습니다: {review_filepath}"
        )

    review_df = pd.read_csv(review_filepath)
    if "review" not in review_df.columns:
        raise ValueError(
            f"'{review_filepath.name}' 파일에 review 컬럼이 없습니다. "
            "크롤러 출력 파일(예: *_reviews.csv)을 사용하세요."
        )

    analyzer = LLMReviewAnalyzer()
    analyzed_df = analyzer.analyze_dataframe(
        df=review_df,
        review_column="review",
        checkpoint_file=str(analysis_filepath),
        max_workers=max_workers,
    )

    analyzed_df = analyzed_df[analyzed_df["시설_점수"].notna()]
    report = ReportGenerator(analyzed_df)
    report_path = analysis_filepath.parent / f"{store_name}_report.txt"
    report.save_report(str(report_path))

    logger.info("분석 결과: %s", analysis_filepath)
    logger.info("리포트: %s", report_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="리뷰 파이프라인 (크롤링 중심)")
    parser.add_argument("--store", type=str, default=None, help="대상 매장 키")
    parser.add_argument("--all", action="store_true", help="등록된 매장 전체 실행")
    parser.add_argument("--skip-crawling", action="store_true", help="크롤링 단계 건너뜀")
    parser.add_argument(
        "--with-analysis",
        action="store_true",
        help="LLM 분석/리포트 단계까지 실행",
    )
    parser.add_argument("--workers", type=int, default=10, help="LLM 분석 병렬 작업 수")
    parser.add_argument("--wait-time", type=int, default=CRAWLING_CONFIG["wait_time"])
    parser.add_argument("--click-wait", type=int, default=CRAWLING_CONFIG["click_wait"])
    parser.add_argument("--headless", action="store_true", help="헤드리스 크롬 사용")

    args = parser.parse_args()

    if args.all:
        targets = list(STORES.keys())
    elif args.store:
        targets = [args.store]
    else:
        targets = [list(STORES.keys())[0]]
        logger.info("매장이 지정되지 않아 기본 매장(%s) 사용", targets[0])

    for key in targets:
        run_store_pipeline(
            store_key=key,
            skip_crawling=args.skip_crawling,
            with_analysis=args.with_analysis,
            max_workers=args.workers,
            wait_time=args.wait_time,
            click_wait=args.click_wait,
            headless=args.headless,
        )


if __name__ == "__main__":
    main()
