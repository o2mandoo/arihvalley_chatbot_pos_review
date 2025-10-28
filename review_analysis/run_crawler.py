"""
크롤링 실행 스크립트
"""
import sys
from pathlib import Path

# 모듈 경로 추가
sys.path.append(str(Path(__file__).parent))

from modules.crawler import NaverReviewCrawler
from config.config import STORES, CRAWLING_CONFIG, get_review_filepath
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def crawl_store(store_key: str):
    """
    특정 매장의 리뷰 크롤링

    Args:
        store_key: STORES 딕셔너리의 키 (예: "강남점")
    """
    if store_key not in STORES:
        logger.error(f"매장 정보를 찾을 수 없습니다: {store_key}")
        logger.info(f"사용 가능한 매장: {list(STORES.keys())}")
        return

    store_info = STORES[store_key]
    url = store_info["url"]
    store_name = store_info["store_name"]

    logger.info(f"=== {store_key} 크롤링 시작 ===")
    logger.info(f"매장명: {store_name}")
    logger.info(f"URL: {url}")

    # 크롤러 실행
    crawler = NaverReviewCrawler(headless=CRAWLING_CONFIG["headless"])

    filepath = get_review_filepath(store_name)

    review_count = crawler.crawl_and_save(
        url=url,
        filepath=str(filepath),
        wait_time=CRAWLING_CONFIG["wait_time"],
        click_wait=CRAWLING_CONFIG["click_wait"]
    )

    logger.info(f"=== 크롤링 완료 ===")
    logger.info(f"수집된 리뷰: {review_count}개")
    logger.info(f"저장 위치: {filepath}")


def crawl_all_stores():
    """모든 매장의 리뷰 크롤링"""
    logger.info(f"=== 전체 매장 크롤링 시작 ===")
    logger.info(f"총 {len(STORES)}개 매장")

    for store_key in STORES.keys():
        try:
            crawl_store(store_key)
        except Exception as e:
            logger.error(f"{store_key} 크롤링 중 오류: {e}")

    logger.info("=== 전체 크롤링 완료 ===")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="네이버 리뷰 크롤링")
    parser.add_argument(
        "--store",
        type=str,
        help=f"크롤링할 매장 (사용 가능: {list(STORES.keys())})",
        default=None
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="모든 매장 크롤링"
    )

    args = parser.parse_args()

    if args.all:
        crawl_all_stores()
    elif args.store:
        crawl_store(args.store)
    else:
        # 기본값: 첫 번째 매장 크롤링
        first_store = list(STORES.keys())[0]
        logger.info(f"매장이 지정되지 않아 기본 매장({first_store})을 크롤링합니다.")
        crawl_store(first_store)
