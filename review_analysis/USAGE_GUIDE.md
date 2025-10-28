# 사용 가이드

## 빠른 시작

### 1단계: 환경 설정
```bash
cd total_pipeline
pip install -r requirements.txt
```

.env 파일에 OpenAI API 키 설정:
```
OPENAI_API_KEY=your-api-key-here
```

### 2단계: 매장 정보 설정
[config/config.py](config/config.py)에서 분석할 매장 URL 설정

### 3단계: 전체 파이프라인 실행
```bash
python run_pipeline.py --store 강남점
```

## 실행 예시

### 예시 1: 처음 사용 (전체 파이프라인)
```bash
# 크롤링 → 분석 → 리포트 생성
python run_pipeline.py --store 강남점

# 예상 소요 시간:
# - 크롤링: 3-5분 (리뷰 수에 따라)
# - 분석 (100개): 2-3분
# 총 5-8분
```

### 예시 2: 정기 업데이트 (증분 업데이트)
```bash
# 2주 후 다시 실행
python run_pipeline.py --store 강남점

# 자동으로:
# - 새로운 리뷰만 크롤링
# - 중복 제거
# - 새로운 리뷰만 분석
# - 전체 리포트 재생성
```

### 예시 3: 분석만 다시 실행
```bash
# 크롤링은 건너뛰고 기존 데이터로 분석만
python run_pipeline.py --store 강남점 --skip-crawling
```

### 예시 4: 리포트만 재생성
```bash
# 크롤링과 분석 모두 건너뛰고 리포트만 재생성
python run_pipeline.py --store 강남점 --skip-crawling --skip-analysis
```

### 예시 5: 빠른 크롤링 테스트
```bash
# 일부만 크롤링해서 테스트
python quick_test.py
```

## 출력 파일 설명

### 1. 리뷰 데이터 (`data/아리계곡_강남점_reviews.csv`)
크롤링된 원본 리뷰:
```csv
review,review_hash
"강남역 아리계곡 두번째 방문...",abc123...
```

### 2. 분석 결과 (`results/아리계곡_강남점_analysis.csv`)
LLM 분석 결과:
```csv
review,시설_점수,서비스_점수,맛_점수,긍정키워드,부정키워드,...
"강남역 아리계곡...",4,3,5,"분위기 좋음,깔끔함","","..."
```

### 3. 리포트 (`results/아리계곡_강남점_report.txt`)
종합 분석 리포트:
```
🎯 리뷰 분석 종합 리포트
==============================

📊 분석 개요
  - 총 리뷰 수: 100건

🏆 전체 평균 점수
  - 시설: 3.80/5
  - 서비스: 2.00/5
  - 맛: 4.00/5

🟢 주요 강점
  ...

🎯 실행 가능한 액션 플랜
  ...
```

## 고급 사용법

### 병렬 처리 최적화
```bash
# 더 빠르게 (비용 증가)
python run_pipeline.py --store 강남점 --workers 20

# 더 느리게 (비용 절감)
python run_pipeline.py --store 강남점 --workers 5
```

### 여러 매장 분석
```bash
# 각 매장별로 실행
python run_pipeline.py --store 강남점
python run_pipeline.py --store 종로점
python run_pipeline.py --store 홍대점
```

## 트러블슈팅

### 문제: 크롤링 실패
**증상**: "iframe 진입 실패" 또는 요소를 찾을 수 없음

**해결책**:
1. [config/config.py](config/config.py)에서 `wait_time` 증가:
   ```python
   CRAWLING_CONFIG = {
       "wait_time": 10,  # 5 → 10으로 증가
       "click_wait": 5,  # 3 → 5로 증가
   }
   ```

2. Chrome 업데이트 및 ChromeDriver 재설치

### 문제: LLM 분석 실패
**증상**: OpenAI API 오류

**해결책**:
1. .env 파일의 API 키 확인
2. OpenAI 계정 할당량 확인
3. 병렬 작업 수 감소: `--workers 3`

### 문제: 메모리 부족
**증상**: 큰 데이터셋 처리 시 메모리 오류

**해결책**:
1. 병렬 작업 수 감소: `--workers 5`
2. 데이터를 여러 배치로 나누어 처리

## 비용 계산

### OpenAI API 비용 (gpt-4o-mini)
- 입력: $0.150 / 1M tokens
- 출력: $0.600 / 1M tokens

### 예상 비용
- 리뷰 1개: 약 500 tokens (입력) + 300 tokens (출력) = 약 $0.001
- 100개 리뷰: 약 $0.10
- 1000개 리뷰: 약 $1.00

### 비용 절감 팁
1. 증분 업데이트 사용 (새로운 리뷰만 분석)
2. 병렬 작업 수 감소 (`--workers` 낮추기)
3. 체크포인트 활용 (중단된 작업 이어하기)

## 자동화

### cron으로 정기 실행 (Linux/Mac)
```bash
# 매일 오전 9시에 실행
0 9 * * * cd /path/to/total_pipeline && python run_pipeline.py --store 강남점
```

### Task Scheduler로 정기 실행 (Windows)
1. 작업 스케줄러 열기
2. 새 작업 만들기
3. 트리거: 매일 오전 9시
4. 동작: `python run_pipeline.py --store 강남점`

## 문의

문제가 발생하면 이슈를 등록해주세요!
