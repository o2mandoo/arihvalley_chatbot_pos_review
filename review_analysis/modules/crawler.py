"""
네이버 지도 리뷰 크롤러 모듈
"""
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup as BS
import time
import pandas as pd
from typing import List, Optional
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class NaverReviewCrawler:
    """네이버 지도 리뷰 크롤러 클래스"""

    def __init__(self, headless: bool = False):
        """
        크롤러 초기화

        Args:
            headless: 브라우저를 헤드리스 모드로 실행할지 여부
        """
        self.headless = headless
        self.driver = None

    def _setup_driver(self) -> webdriver.Chrome:
        """Chrome 드라이버 설정"""
        options = Options()

        if not self.headless:
            options.add_argument("--start-maximized")
        else:
            options.add_argument("--headless")

        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("detach", True)
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        driver = webdriver.Chrome(service=Service(), options=options)
        logger.info("Chrome 드라이버 설정 완료")
        return driver

    def crawl_reviews(self, url: str, wait_time: int = 5, click_wait: int = 3) -> List[str]:
        """
        네이버 지도 리뷰 크롤링

        Args:
            url: 네이버 지도 리뷰 페이지 URL
            wait_time: 페이지 로딩 대기 시간 (초)
            click_wait: 더보기 버튼 클릭 후 대기 시간 (초)

        Returns:
            리뷰 텍스트 리스트
        """
        try:
            self.driver = self._setup_driver()
            self.driver.get(url)
            logger.info(f"페이지 접속: {url}")

            time.sleep(wait_time)

            # iframe 진입
            try:
                self.driver.switch_to.frame("entryIframe")
                logger.info("iframe 진입 완료")
            except Exception as e:
                logger.error(f"iframe 진입 실패: {e}")
                return []

            # "더보기" 버튼 반복 클릭
            click_count = 0
            max_attempts = 100  # 무한 루프 방지

            while click_count < max_attempts:
                try:
                    time.sleep(click_wait)
                    # 더보기 버튼 찾기 (CSS 선택자는 변경될 수 있음)
                    element = self.driver.find_element(
                        By.CSS_SELECTOR,
                        "#app-root > div > div > div:nth-child(7) > div:nth-child(3) > div.place_section.k1QQ5 > div.NSTUp > div > a > span"
                    )
                    element.click()
                    click_count += 1
                    logger.info(f"더보기 클릭 성공 ({click_count}회)")

                except Exception as e:
                    logger.info(f"더보기 버튼 없음 또는 클릭 종료: {e}")
                    break

            # 페이지 파싱
            time.sleep(3)
            bs = BS(self.driver.page_source, 'html.parser')
            reviews = bs.find_all("div", 'pui__vn15t2')

            # 리뷰 텍스트 추출
            review_texts = [review.get_text(separator=" ").strip() for review in reviews]
            logger.info(f"총 {len(review_texts)}개의 리뷰 수집 완료")

            return review_texts

        except Exception as e:
            logger.error(f"크롤링 중 오류 발생: {e}")
            return []

        finally:
            if self.driver:
                self.driver.quit()
                logger.info("드라이버 종료")

    def save_to_csv(self, reviews: List[str], filepath: str) -> bool:
        """
        리뷰를 CSV 파일로 저장

        Args:
            reviews: 리뷰 텍스트 리스트
            filepath: 저장할 파일 경로

        Returns:
            저장 성공 여부
        """
        try:
            df = pd.DataFrame({'review': reviews})
            df.to_csv(filepath, index=False, encoding="utf-8-sig")
            logger.info(f"리뷰 데이터 저장 완료: {filepath}")
            return True
        except Exception as e:
            logger.error(f"CSV 저장 실패: {e}")
            return False

    def crawl_and_save(self, url: str, filepath: str, **kwargs) -> int:
        """
        크롤링 후 바로 CSV로 저장

        Args:
            url: 네이버 지도 리뷰 페이지 URL
            filepath: 저장할 파일 경로
            **kwargs: crawl_reviews에 전달할 추가 인자

        Returns:
            수집된 리뷰 개수
        """
        reviews = self.crawl_reviews(url, **kwargs)
        if reviews:
            self.save_to_csv(reviews, filepath)
        return len(reviews)


def main():
    """테스트용 메인 함수"""
    # 예시 URL (실제 사용 시 변경 필요)
    test_url = "https://map.naver.com/p/search/%EC%95%84%EB%A6%AC%EA%B3%84%EA%B3%A1/place/1377026954?c=15.00,0,0,0,dh&placePath=/review"

    crawler = NaverReviewCrawler(headless=False)
    review_count = crawler.crawl_and_save(
        url=test_url,
        filepath="test_reviews.csv",
        wait_time=5,
        click_wait=3
    )

    print(f"수집된 리뷰 개수: {review_count}")


if __name__ == "__main__":
    main()
