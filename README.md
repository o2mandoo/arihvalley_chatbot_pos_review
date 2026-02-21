# 아리계곡 리뷰·매출 분석 챗봇

점주 친화형 챗봇 데모 프로젝트입니다.  
웹에서 질문하면 리뷰 분석/매출 분석을 자동 분류하고, `text2SQL + 인사이트 + 차트` 형태로 답변합니다.

## 0) 프로젝트 의도

이 프로젝트는 "데이터가 있어도 점주가 빠르게 의사결정하기 어렵다"는 문제를 해결하기 위한 데모입니다.

- 목표 1: 분석가 없이도 자연어 질문으로 바로 확인
  - 복잡한 SQL/BI 도구 없이, 채팅으로 바로 답을 받도록 설계
- 목표 2: 단순 집계가 아닌 실행 가능한 인사이트 제공
  - 리뷰: 반복 불만, 숨은 불만, 재방문 신호까지 포착
  - 매출: 기간 비교와 원인 분해(채널/카테고리/요일/시간대)까지 제시
- 목표 3: 시연 친화적인 속도와 완성도
  - 데모 단계에서는 DB 구축보다 속도를 우선해 CSV/엑셀을 DuckDB 메모리로 즉시 분석
  - 응답은 표/차트/마크다운으로 직관적으로 보여주고, SQL은 필요 시 토글로만 노출
- 목표 4: 점주 친화 UX
  - 개발자 용어를 줄이고, “무슨 문제가 있고 무엇을 개선해야 하는지” 중심으로 답변

한 줄 요약:  
`점주가 바로 이해하고 행동할 수 있는 데이터 챗봇을, 빠른 속도로 실전 시연 가능하게 구현`하는 것이 핵심 의도입니다.

## 1) 현재 구현 범위

- 리뷰 분석 챗봇 (`/`)
  - 리뷰 CSV 기반 text2SQL
  - 반복 부정 신호/숨은 불만/재방문 키워드 분석
  - 표 + 인사이트 + 차트 출력
  - 분석 근거 SQL은 토글(`분석 근거(SQL) 보기`)로만 노출
- 매출 분석 챗봇 (`/sales`)
  - 매출 엑셀 기반 text2SQL (DuckDB 메모리)
  - 기간 비교/원인 분해 인사이트
  - 표 + 인사이트 + 차트 출력
- 프론트
  - Next.js + `ai`(`@vercel/ai`) + `react-markdown` + `remark-gfm`
  - 스트리밍 응답, 실시간 생성 UI, 챗 UX 애니메이션
- 백엔드
  - FastAPI
  - OpenAI 모델 기본값: `gpt-5-mini` (속도 우선)
  - `OPENAI_API_KEY`는 `.env` 없으면 `~/.zshrc`에서 fallback 로드

## 2) 프로젝트 구조

- `review_chat_frontend/`: Next.js 프론트엔드
- `review_chat_backend/`: FastAPI 백엔드
- `review_analysis/`: 리뷰 데이터/크롤링 관련 리소스
- `revenue-data/`: 매출 엑셀 리포트 위치
- `docker-compose.yml`: 프론트+백엔드 통합 실행

## 3) 데이터 파일 준비 (중요)

이 저장소는 민감 데이터 보호를 위해 리뷰/매출 데이터가 git에 포함되지 않습니다.  
로컬에 아래 파일을 직접 넣어주세요.

- 리뷰: `review_analysis/data/아리계곡_통합_.csv`
- 매출: `revenue-data/왕십리_매출리포트-260221.xlsx`

참고:
- 기본 엑셀 암호는 `7055`로 동작하도록 설정되어 있습니다.
- `-decrypted.xlsx`가 있으면 자동 폴백 로직도 있습니다.

## 4) 로컬 실행 (권장)

### 4-1. 백엔드 실행

```bash
cd review_chat_backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

### 4-2. 프론트 실행

```bash
cd review_chat_frontend
cp .env.example .env.local
npm install
npm run dev
```

접속:
- 리뷰 챗봇: `http://localhost:5173`
- 매출 챗봇: `http://localhost:5173/sales`

### 4-3. Docker로 한 번에 실행 (선택)

```bash
docker compose up --build
```

- 프론트: `http://localhost:5173`
- 백엔드: `http://localhost:8000`

## 5) 환경변수

### 5-1. 백엔드 (`review_chat_backend`)

필수:
- `OPENAI_API_KEY`

주요 옵션:
- `OPENAI_MODEL` (기본: `gpt-5-mini`)
- `OPENAI_TEMPERATURE` (기본: `0.45`)
- `EXCEL_PASSWORD` (기본: `7055`)
- `SALES_REPORT_FILE` (기본: `왕십리_매출리포트-260221.xlsx`)
- `SALES_BRANCH_NAME` (기본: `왕십리한양대점`)

### 5-2. 프론트 (`review_chat_frontend/.env.local`)

- `REVIEW_BACKEND_URL` (로컬 기본 연결 URL)
- `REVIEW_BACKEND_URL_PREVIEW` (Vercel Preview 전용)
- `REVIEW_BACKEND_URL_PRODUCTION` (Vercel Production 전용)
- `NEXT_PUBLIC_SALES_CHAT_URL` (선택: 리뷰->매출 링크 강제)
- `NEXT_PUBLIC_REVIEW_CHAT_URL` (선택: 매출->리뷰 링크 강제)

주의:
- URL 값에는 `/api/chat`를 붙이지 말고 도메인만 넣어야 합니다.

## 6) 배포 가이드

### 6-1. 프론트 (Vercel)

Vercel 프로젝트 환경변수에 아래 등록:

- `REVIEW_BACKEND_URL_PREVIEW=https://<preview-backend-domain>`
- `REVIEW_BACKEND_URL_PRODUCTION=https://<production-backend-domain>`

배포 후 확인:

```bash
curl https://<vercel-domain>/api/runtime
```

응답의 `vercelEnv`, `source`, `backendUrl`이 의도대로인지 확인합니다.

### 6-2. 백엔드 (Google Cloud Run)

현재 운영 URL:

- `https://ari-chat-backend-109390027711.us-east1.run.app`

배포 흐름(요약):

1. Artifact Registry 푸시용 인증
2. `linux/amd64` 이미지 빌드/푸시
3. `gcloud run deploy`로 서비스 업데이트

예시:

```bash
PROJECT_ID=project-71091d05-55c5-4f1a-846
REGION=us-east1
SERVICE=ari-chat-backend
REPO=ari-backend
TAG=$(date +%Y%m%d-%H%M%S)-amd64
IMAGE_URI=${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${SERVICE}:${TAG}

gcloud auth configure-docker ${REGION}-docker.pkg.dev
docker buildx build --platform linux/amd64 --load -t "${IMAGE_URI}" -f review_chat_backend/Dockerfile .
docker push "${IMAGE_URI}"
gcloud run deploy "${SERVICE}" --image "${IMAGE_URI}" --region "${REGION}" --project "${PROJECT_ID}"
```

헬스체크:

```bash
curl https://ari-chat-backend-109390027711.us-east1.run.app/health
```

## 7) API 요약

백엔드:
- `GET /health`
- `POST /api/chat` (리뷰/매출 자동 분류 처리)
- `GET /api/sales/source`
- `POST /api/sales/source`

프론트 내부 API:
- `POST /api/chat` (리뷰 프록시)
- `POST /api/sales-chat` (매출 프록시)
- `GET /api/runtime` (Vercel 환경 라우팅 확인)

## 8) 점검용 추천 질문

리뷰:
1. `최근 리뷰에서 자주 반복되는 아쉬운 점을 알려줘`
2. `재방문 키워드는 뭐가 있어? 지점별로도 알려줘`
3. `지점별로 가장 많이 언급된 불만을 비교해줘`

매출:
1. `최근 7일 총매출과 주문 건수를 알려줘`
2. `최근 14일 일자별 매출 추이를 보여줘`
3. `전월 대비 이번달 매출 변화를 알려줘`

## 9) 자주 발생한 이슈

- `Failed to parse URL from REVIEW_BACKEND_URL_PRODUCTION/api/chat`
  - 원인: 환경변수 값에 `/api/chat`까지 넣은 경우
  - 해결: 도메인만 입력
- Cloud Run 배포 시 `manifest type ... must support amd64/linux`
  - 해결: `docker buildx build --platform linux/amd64 ...`
- Secret 접근 권한 오류
  - 해결: Cloud Run 서비스 계정에 `roles/secretmanager.secretAccessor` 부여

## 10) 시연 이미지

아래 섹션에 캡처 이미지를 추가해서 사용하세요.

```md
### 리뷰 분석 화면
![review-demo](docs/images/review-demo.png)

### 매출 분석 화면
![sales-demo](docs/images/sales-demo.png)
```
