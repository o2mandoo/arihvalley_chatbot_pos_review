"""
파이프라인 모듈 통합 테스트 (LLM 분석 포함)
"""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))

import pandas as pd
from modules import ReviewDataManager, LLMReviewAnalyzer, ReportGenerator

print("=" * 100)
print("🧪 파이프라인 모듈 통합 테스트")
print("=" * 100)

# 1. 데이터 관리 모듈 테스트
print("\n" + "=" * 100)
print("1️⃣ 데이터 관리 모듈 테스트")
print("=" * 100)

test_reviews = [
    "강남역 아리계곡 두번째 방문인데 분위기부터 진짜 좋아유!! 음식도 맛있어요.",
    "친구가 예전에 먹었는데 맛있다고 해서 저 데려왔어요!! 국물 넘 맛잇네요.",
    "이전에 모임으로 욌었는데 진흙오리전병이 너무 맛있어서..또 왔어요!!"
]

data_manager = ReviewDataManager("test_data.csv")
data_manager.load_data()
added = data_manager.merge_and_update(test_reviews)

print(f"✓ 추가된 리뷰: {added}건")
print(f"✓ 총 리뷰: {data_manager.get_statistics()['total_reviews']}건")

# 2. LLM 분석 모듈 테스트 (샘플 5개만)
print("\n" + "=" * 100)
print("2️⃣ LLM 분석 모듈 테스트 (샘플 5개)")
print("=" * 100)

# 기존 크롤링된 데이터 사용
data_file = Path(__file__).parent / "data" / "아리계곡_강남점_reviews.csv"

if data_file.exists():
    df = pd.read_csv(data_file)
    print(f"데이터 로드: {len(df)}건")

    # 처음 5개만 분석
    sample_df = df.head(5).copy()
    print(f"샘플 분석 대상: {len(sample_df)}건")

    try:
        analyzer = LLMReviewAnalyzer()
        print("✓ LLM 분석기 초기화 완료")

        analyzed_df = analyzer.analyze_dataframe(
            df=sample_df,
            review_column='review',
            checkpoint_file="test_analysis.csv",
            max_workers=3
        )

        print("✓ 분석 완료!")
        print(f"\n분석 결과 샘플:")
        print(analyzed_df[['review', '시설_점수', '서비스_점수', '맛_점수', '전체요약']].head(3))

        # 3. 리포트 생성 모듈 테스트
        print("\n" + "=" * 100)
        print("3️⃣ 리포트 생성 모듈 테스트")
        print("=" * 100)

        # 분석된 데이터만 필터링
        analyzed_only = analyzed_df[analyzed_df['시설_점수'].notna()]

        if len(analyzed_only) > 0:
            report_gen = ReportGenerator(analyzed_only)
            report = report_gen.generate_full_report()

            print("\n" + report)

            # 리포트 저장
            report_gen.save_report("test_report.txt")
            print("\n✓ 리포트 저장: test_report.txt")
        else:
            print("⚠️  분석된 데이터가 없어 리포트를 생성할 수 없습니다.")

    except Exception as e:
        print(f"✗ LLM 분석 실패: {e}")
        import traceback
        traceback.print_exc()
else:
    print(f"⚠️  테스트 데이터 파일이 없습니다: {data_file}")
    print("먼저 크롤링을 실행하세요: python quick_test.py")

print("\n" + "=" * 100)
print("✅ 모듈 테스트 완료")
print("=" * 100)
