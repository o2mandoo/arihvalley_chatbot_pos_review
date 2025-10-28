"""
크롤러 간단 테스트 스크립트
실제 크롤링 전에 모듈 import 및 기본 기능 테스트
"""
import sys
from pathlib import Path

# 모듈 경로 추가
sys.path.append(str(Path(__file__).parent))

print("=== 모듈 Import 테스트 ===")

try:
    from modules.crawler import NaverReviewCrawler
    print("✓ NaverReviewCrawler import 성공")
except Exception as e:
    print(f"✗ NaverReviewCrawler import 실패: {e}")
    sys.exit(1)

try:
    from config.config import STORES, CRAWLING_CONFIG, get_review_filepath
    print("✓ config 모듈 import 성공")
except Exception as e:
    print(f"✗ config 모듈 import 실패: {e}")
    sys.exit(1)

print("\n=== 설정 확인 ===")
print(f"등록된 매장 수: {len(STORES)}")
for store_key, store_info in STORES.items():
    print(f"  - {store_key}: {store_info['store_name']}")

print(f"\n크롤링 설정:")
print(f"  - wait_time: {CRAWLING_CONFIG['wait_time']}초")
print(f"  - click_wait: {CRAWLING_CONFIG['click_wait']}초")
print(f"  - headless: {CRAWLING_CONFIG['headless']}")

print("\n=== 크롤러 인스턴스 생성 테스트 ===")
try:
    crawler = NaverReviewCrawler(headless=True)
    print("✓ 크롤러 인스턴스 생성 성공")
except Exception as e:
    print(f"✗ 크롤러 인스턴스 생성 실패: {e}")
    sys.exit(1)

print("\n=== 파일 경로 테스트 ===")
for store_key, store_info in STORES.items():
    filepath = get_review_filepath(store_info['store_name'])
    print(f"{store_key}: {filepath}")

print("\n=== 기본 테스트 완료 ===")
print("실제 크롤링을 실행하려면 다음 명령어를 사용하세요:")
print("  python run_crawler.py --store 강남점")
print("  또는")
print("  python run_crawler.py --all")
