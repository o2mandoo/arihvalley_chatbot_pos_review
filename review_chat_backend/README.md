# Review Chat Backend

리뷰 분석 전용 챗봇 백엔드입니다.

## 특징

- 도메인 분류: `리뷰` vs `매출`
- 리뷰 질의는 `text2SQL` 방식으로 `DuckDB(reviews)` 질의 실행
- 반드시 `review_analysis/data/아리계곡_통합_.csv`를 데이터 소스로 사용
- 결과를 마크다운 표 형태로 반환
- 긍정 속 숨은 부정 신호 + 반복성(반복적/산발적) 해석 포함

## 실행

```bash
cd review_chat_backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

## 환경변수

- `OPENAI_API_KEY` (필수)
- `OPENAI_MODEL` (기본: `gpt-5-mini`)

`OPENAI_API_KEY`가 없으면 `.env` -> `~/.zshrc` 순서로 fallback 로드합니다.
