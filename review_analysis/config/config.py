"""
파이프라인 설정 파일
"""
import os
from pathlib import Path

# 프로젝트 루트 경로
PROJECT_ROOT = Path(__file__).parent.parent

# 데이터 경로
DATA_DIR = PROJECT_ROOT / "data"
RESULTS_DIR = PROJECT_ROOT / "results"

# 크롤링 설정
CRAWLING_CONFIG = {
    "wait_time": 5,  # 페이지 로딩 대기 시간 (초)
    "click_wait": 3,  # 더보기 버튼 클릭 후 대기 시간 (초)
    "headless": False,  # 헤드리스 모드 사용 여부
}

# 매장 정보 (예시)
STORES = {
    "강남점": {
        "url": "https://map.naver.com/p/search/%EC%95%84%EB%A6%AC%EA%B3%84%EA%B3%A1/place/1377026954?c=15.00,0,0,0,dh&placePath=/review",
        "place_id": "1377026954",
        "store_name": "아리계곡_강남점"
    },
    # 추가 매장 정보는 여기에 추가
}

# 데이터 파일 경로
def get_review_filepath(store_name: str) -> Path:
    """리뷰 CSV 파일 경로 반환"""
    return DATA_DIR / f"{store_name}_reviews.csv"

def get_analysis_filepath(store_name: str) -> Path:
    """분석 결과 CSV 파일 경로 반환"""
    return RESULTS_DIR / f"{store_name}_analysis.csv"

# 디렉토리 생성
DATA_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
