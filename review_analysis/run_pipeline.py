#!/usr/bin/env python3
"""
전체 리뷰 분석 파이프라인 실행 스크립트
크롤링 → 데이터 관리 → LLM 분석 → 리포트 생성
"""
import sys
from pathlib import Path
import logging
import argparse

# 모듈 경로 추가
sys.path.append(str(Path(__file__).parent))

from modules import NaverReviewCrawler, ReviewDataManager, LLMReviewAnalyzer, ReportGenerator
from config.config import STORES, CRAWLING_CONFIG, get_review_filepath, get_analysis_filepath

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_full_pipeline(
    store_key: str,
    skip_crawling: bool = False,
    skip_analysis: bool = False,
    max_workers: int = 10
):
    """
    전체 파이프라인 실행

    Args:
        store_key: 매장 키 (예: "강남점")
        skip_crawling: 크롤링 건너뛰기
        skip_analysis: 분석 건너뛰기
        max_workers: LLM 분석 병렬 작업 수
    """
    if store_key not in STORES:
        logger.error(f"매장 정보를 찾을 수 없습니다: {store_key}")
        logger.info(f"사용 가능한 매장: {list(STORES.keys())}")
        return

    store_info = STORES[store_key]
    store_name = store_info["store_name"]

    logger.info("=" * 100)
    logger.info("🚀 리뷰 분석 파이프라인 시작")
    logger.info("=" * 100)
    logger.info(f"매장: {store_key} ({store_name})")
    logger.info(f"크롤링: {'건너뛰기' if skip_crawling else '실행'}")
    logger.info(f"분석: {'건너뛰기' if skip_analysis else '실행'}")
    logger.info("=" * 100)

    # 파일 경로
    review_filepath = get_review_filepath(store_name)
    analysis_filepath = get_analysis_filepath(store_name)

    # ========================================
    # 1단계: 크롤링
    # ========================================
    if not skip_crawling:
        logger.info("\n" + "=" * 100)
        logger.info("📡 1단계: 리뷰 크롤링")
        logger.info("=" * 100)

        url = store_info["url"]
        crawler = NaverReviewCrawler(headless=CRAWLING_CONFIG["headless"])

        try:
            # 크롤링 실행
            new_reviews = crawler.crawl_reviews(
                url=url,
                wait_time=CRAWLING_CONFIG["wait_time"],
                click_wait=CRAWLING_CONFIG["click_wait"]
            )

            logger.info(f"크롤링 완료: {len(new_reviews)}개 리뷰 수집")

            # ========================================
            # 2단계: 데이터 관리 (증분 업데이트)
            # ========================================
            logger.info("\n" + "=" * 100)
            logger.info("💾 2단계: 데이터 관리 (증분 업데이트)")
            logger.info("=" * 100)

            data_manager = ReviewDataManager(str(review_filepath))
            data_manager.load_data()

            # 기존 데이터 통계
            old_stats = data_manager.get_statistics()
            logger.info(f"기존 리뷰: {old_stats['total_reviews']}건")

            # 새로운 리뷰 추가 (중복 제거)
            added_count = data_manager.merge_and_update(new_reviews)

            # 새로운 통계
            new_stats = data_manager.get_statistics()
            logger.info(f"새로 추가된 리뷰: {added_count}건")
            logger.info(f"총 리뷰: {new_stats['total_reviews']}건")

        except Exception as e:
            logger.error(f"크롤링 중 오류 발생: {e}")
            import traceback
            traceback.print_exc()
            return
    else:
        logger.info("\n크롤링 단계를 건너뜁니다.")

    # ========================================
    # 3단계: LLM 분석
    # ========================================
    if not skip_analysis:
        logger.info("\n" + "=" * 100)
        logger.info("🤖 3단계: LLM 기반 리뷰 분석")
        logger.info("=" * 100)

        try:
            # 리뷰 데이터 로드
            import pandas as pd
            review_df = pd.read_csv(review_filepath)
            logger.info(f"분석 대상 리뷰: {len(review_df)}건")

            # LLM 분석기 초기화
            analyzer = LLMReviewAnalyzer()

            # 분석 실행 (체크포인트 지원)
            analyzed_df = analyzer.analyze_dataframe(
                df=review_df,
                review_column='review',
                checkpoint_file=str(analysis_filepath),
                max_workers=max_workers
            )

            logger.info(f"분석 완료: {str(analysis_filepath)}")

        except Exception as e:
            logger.error(f"LLM 분석 중 오류 발생: {e}")
            import traceback
            traceback.print_exc()
            return
    else:
        logger.info("\n분석 단계를 건너뜁니다.")

    # ========================================
    # 4단계: 리포트 생성
    # ========================================
    logger.info("\n" + "=" * 100)
    logger.info("📊 4단계: 리포트 생성")
    logger.info("=" * 100)

    try:
        import pandas as pd
        analyzed_df = pd.read_csv(analysis_filepath)

        # 분석 완료된 데이터만 필터링
        analyzed_df = analyzed_df[analyzed_df['시설_점수'].notna()]
        logger.info(f"분석 완료된 리뷰: {len(analyzed_df)}건")

        # 리포트 생성
        report_gen = ReportGenerator(analyzed_df)
        report = report_gen.generate_full_report()

        # 콘솔 출력
        print("\n" + report)

        # 파일 저장
        report_filepath = analysis_filepath.parent / f"{store_name}_report.txt"
        report_gen.save_report(str(report_filepath))

        logger.info(f"리포트 저장: {report_filepath}")

    except Exception as e:
        logger.error(f"리포트 생성 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return

    # ========================================
    # 완료
    # ========================================
    logger.info("\n" + "=" * 100)
    logger.info("✅ 파이프라인 완료!")
    logger.info("=" * 100)
    logger.info(f"리뷰 데이터: {review_filepath}")
    logger.info(f"분석 결과: {analysis_filepath}")
    logger.info(f"리포트: {report_filepath}")
    logger.info("=" * 100)


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="리뷰 분석 파이프라인")

    parser.add_argument(
        "--store",
        type=str,
        default=None,
        help=f"분석할 매장 (예: {list(STORES.keys())[0]})"
    )
    parser.add_argument(
        "--skip-crawling",
        action="store_true",
        help="크롤링 건너뛰기 (기존 데이터 사용)"
    )
    parser.add_argument(
        "--skip-analysis",
        action="store_true",
        help="LLM 분석 건너뛰기 (기존 분석 사용)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="LLM 분석 병렬 작업 수 (기본: 10)"
    )

    args = parser.parse_args()

    # 매장 선택
    if args.store:
        store_key = args.store
    else:
        store_key = list(STORES.keys())[0]
        logger.info(f"매장이 지정되지 않아 기본 매장({store_key})을 사용합니다.")

    # 파이프라인 실행
    run_full_pipeline(
        store_key=store_key,
        skip_crawling=args.skip_crawling,
        skip_analysis=args.skip_analysis,
        max_workers=args.workers
    )


if __name__ == "__main__":
    main()
