"""
수집된 데이터 확인
"""
import pandas as pd
from pathlib import Path

data_file = Path(__file__).parent / "data" / "아리계곡_강남점_reviews.csv"

print("=== 크롤링 데이터 확인 ===\n")

df = pd.read_csv(data_file)

print(f"총 리뷰 개수: {len(df)}개")
print(f"\n데이터 구조:")
print(df.info())

print(f"\n리뷰 길이 통계:")
df['review_length'] = df['review'].str.len()
print(df['review_length'].describe())

print(f"\n처음 3개 리뷰:")
for i, row in df.head(3).iterrows():
    print(f"\n[리뷰 {i+1}]")
    preview = row['review'][:150] + "..." if len(row['review']) > 150 else row['review']
    print(preview)

print("\n" + "="*50)
print("✓ 크롤링 성공!")
print(f"✓ 저장 위치: {data_file}")
print("="*50)
