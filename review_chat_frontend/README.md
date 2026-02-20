# Review Chat Frontend (Next.js)

Vercel 배포를 전제로 만든 Next.js 프론트엔드입니다.

## Stack

- Next.js (App Router)
- `ai` (`@vercel/ai`)의 `useChat`
- `react-markdown` + `remark-gfm`

## Local

```bash
cd review_chat_frontend
cp .env.example .env.local
npm install
npm run dev
```

- 앱: `http://localhost:5173`
- 기본 백엔드: `http://localhost:8000`

## Vercel 배포

1. Vercel에서 `review_chat_frontend` 디렉터리를 프로젝트 루트로 연결
2. Environment Variable 추가:
   - `REVIEW_BACKEND_URL_PREVIEW=https://<preview-backend-domain>`
   - `REVIEW_BACKEND_URL_PRODUCTION=https://<production-backend-domain>`
   - (선택) `REVIEW_BACKEND_URL` : fallback 용도
   - (선택) `NEXT_PUBLIC_SALES_CHAT_URL=https://<sales-chatbot-url>`
3. 각 변수에 환경 지정:
   - `REVIEW_BACKEND_URL_PREVIEW` -> Preview
   - `REVIEW_BACKEND_URL_PRODUCTION` -> Production
4. 배포

CLI로 설정할 경우 예시:

```bash
vercel env add REVIEW_BACKEND_URL_PREVIEW preview
vercel env add REVIEW_BACKEND_URL_PRODUCTION production
```

프론트는 `/api/chat` 라우트에서 환경에 맞는 백엔드를 호출합니다.
따라서 브라우저에 백엔드 URL을 노출하지 않아도 됩니다.

## 분기 규칙

- `VERCEL_ENV=production` 이고 `REVIEW_BACKEND_URL_PRODUCTION`이 있으면 우선 사용
- `VERCEL_ENV=preview` 이고 `REVIEW_BACKEND_URL_PREVIEW`이 있으면 우선 사용
- 없으면 `REVIEW_BACKEND_URL` -> `NEXT_PUBLIC_BACKEND_URL` -> `http://localhost:8000` 순서 fallback

## 확인 방법

- 브라우저: `https://<frontend-domain>/api/runtime`
- 응답에서 아래를 확인:
  - `deploymentEnv` (`preview`, `production`, `local-production` 등)
  - `vercelEnv` (`preview` 또는 `production`, 로컬이면 빈 값)
  - `source` (`REVIEW_BACKEND_URL_PREVIEW` 또는 `REVIEW_BACKEND_URL_PRODUCTION`)
  - `backendUrl` (실제 선택된 백엔드 URL)
- 운영 화면에는 Runtime 체크 UI를 노출하지 않도록 구성되어 있으며, 필요 시 `/api/runtime`로만 확인합니다.
