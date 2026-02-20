# Review + Sales Chat Backend

리뷰/매출 분석 챗봇 백엔드입니다.

## 특징

- 도메인 분류: `리뷰` vs `매출`
- 리뷰 질의는 `text2SQL` 방식으로 `DuckDB(reviews)` 질의 실행
- 반드시 `review_analysis/data/아리계곡_통합_.csv`를 데이터 소스로 사용
- 매출 질의는 `text2SQL` 방식으로 `DuckDB(sales)` 질의 실행
- 기본 매출 소스: `revenue-data/왕십리_매출리포트-260221.xlsx` (암호화 파일 지원, `-decrypted.xlsx` 자동 폴백)
- 엑셀 파서는 `xlsx/xls` 계열을 모두 시도 (`openpyxl` -> `xlrd` 순)
- `/api/sales/source`로 매출 리포트 파일 교체/재로딩 가능 (데모용 업데이트 기능)
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
- `OPENAI_TEMPERATURE` (기본: `0.35`, 높일수록 표현/SQL 생성 변동성 증가)
- `EXCEL_PASSWORD` (기본: `7055`, 암호화된 매출 엑셀 복호화용)
- `SALES_REPORT_FILE` (선택, 기본: `왕십리_매출리포트-260221.xlsx`)
- `SALES_BRANCH_NAME` (선택, 기본: `왕십리한양대점`)

`OPENAI_API_KEY`가 없으면 `.env` -> `~/.zshrc` 순서로 fallback 로드합니다.
