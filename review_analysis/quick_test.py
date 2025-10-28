"""
크롤링 빠른 테스트 - 일부 리뷰만 가져오기
"""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

from modules.crawler import NaverReviewCrawler
from config.config import STORES, get_review_filepath
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 강남점 정보
store_info = STORES["강남점"]
url = store_info["url"]
store_name = store_info["store_name"]

logger.info(f"=== 빠른 테스트 시작 ===")
logger.info(f"매장: {store_name}")
logger.info(f"URL: {url}")
logger.info("※ 더보기 버튼을 3번만 클릭하여 일부 리뷰만 수집합니다")

# 크롤러 생성 및 실행
crawler = NaverReviewCrawler(headless=False)

# 크롤러 클래스의 crawl_reviews 메서드를 직접 호출하되,
# 더보기 클릭 횟수를 제한하기 위해 별도로 크롤링 실행
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup as BS
import time

try:
    crawler.driver = crawler._setup_driver()
    crawler.driver.get(url)
    logger.info(f"페이지 접속 완료")

    time.sleep(5)

    # iframe 진입
    try:
        crawler.driver.switch_to.frame("entryIframe")
        logger.info("iframe 진입 완료")
    except Exception as e:
        logger.error(f"iframe 진입 실패: {e}")
        sys.exit(1)

    # 더보기 버튼 3번만 클릭
    max_clicks = 3
    click_count = 0

    while click_count < max_clicks:
        try:
            time.sleep(2)
            element = crawler.driver.find_element(
                By.CSS_SELECTOR,
                "#app-root > div > div > div:nth-child(7) > div:nth-child(3) > div.place_section.k1QQ5 > div.NSTUp > div > a > span"
            )
            element.click()
            click_count += 1
            logger.info(f"더보기 클릭 성공 ({click_count}/{max_clicks})")
        except Exception as e:
            logger.info(f"더보기 버튼 없음 또는 클릭 종료: {e}")
            break

    # 페이지 파싱
    time.sleep(2)
    bs = BS(crawler.driver.page_source, 'html.parser')
    reviews = bs.find_all("div", 'pui__vn15t2')

    # 리뷰 텍스트 추출
    review_texts = [review.get_text(separator=" ").strip() for review in reviews]
    logger.info(f"총 {len(review_texts)}개의 리뷰 수집 완료")

    # 처음 5개 리뷰만 출력
    logger.info("\n=== 수집된 리뷰 샘플 (처음 5개) ===")
    for i, review in enumerate(review_texts[:5], 1):
        preview = review[:100] + "..." if len(review) > 100 else review
        logger.info(f"{i}. {preview}")

    # CSV 저장
    filepath = get_review_filepath(store_name)
    success = crawler.save_to_csv(review_texts, str(filepath))

    if success:
        logger.info(f"\n=== 크롤링 완료 ===")
        logger.info(f"수집된 리뷰: {len(review_texts)}개")
        logger.info(f"저장 위치: {filepath}")

except Exception as e:
    logger.error(f"크롤링 중 오류: {e}")
    import traceback
    traceback.print_exc()

finally:
    if crawler.driver:
        logger.info("브라우저를 닫습니다 (5초 후)...")
        time.sleep(5)
        crawler.driver.quit()
        logger.info("드라이버 종료")
