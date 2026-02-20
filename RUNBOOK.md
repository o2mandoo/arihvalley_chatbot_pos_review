# FMS Runbook

This document defines one practical, minimal execution path for each component.

## 1) Sales ingestion pipeline (recommended first)

From repo root:

```bash
npm install
cp .env.example .env
```

Edit `.env`:

- `DATABASE_URL`
- `EXCEL_PASSWORD` (if needed)

Generate Prisma client + run migrations:

```bash
npm run prisma:generate
npm run prisma:migrate
```

Run full sales pipeline:

```bash
npm run sales:pipeline
```

Optional sample staffing data:

```bash
npm run sales:seed-staff
```

## 2) NL2SQL chatbot (chatbi_nl2sql)

From repo root:

```bash
python3 -m venv .venv-chatbi
source .venv-chatbi/bin/activate
pip install -r chatbi_nl2sql/requirements.txt
```

Set environment:

```bash
export OPENAI_API_KEY=your-api-key
export OPENAI_MODEL=gpt-5-mini
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/fms
```

If `OPENAI_API_KEY` (or `OPENAI_MODEL`) is already in `~/.zshrc`, ChatBI will auto-load it.

Run:

```bash
python chatbi_nl2sql/chatbi_nl2sql.py
```

## 3) Review crawl + analysis pipeline (review_analysis)

From repo root:

```bash
python3 -m venv .venv-review
source .venv-review/bin/activate
pip install -r review_analysis/requirements.txt
cp review_analysis/.env.example review_analysis/.env
```

Set API key/model in `review_analysis/.env` (or keep them in `~/.zshrc`), then run:

```bash
python review_analysis/run_pipeline.py --store 강남점
```

## Script layout

- Active ingestion path: `scripts/pipeline/`
- Compatibility wrappers: `scripts/*.js`
- Archived legacy scripts: `scripts/legacy/`

## 4) Web chatbot demo (review-first)

This setup splits backend/frontend and focuses on review analytics first.

### Backend (FastAPI)

```bash
cd review_chat_backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

Notes:

- Uses `review_analysis/data/아리계곡_통합_.csv` only.
- Domain router: review vs sales (sales is placeholder response for now).
- Default model: `gpt-5-mini` (speed-priority). Loaded from `.env` or `~/.zshrc`.
- Fast-path SQL templates are used for common review intents (waiting/negative signal/hidden complaints) to reduce latency.

### Frontend (Next.js + @vercel/ai)

```bash
cd review_chat_frontend
cp .env.example .env.local
npm install
npm run dev
```

Open `http://localhost:5173`.

Notes:

- Uses `ai` package (`@vercel/ai`) `useChat` hook.
- Markdown rendering: `react-markdown` + `remark-gfm`.
- Review page: `/`, Sales page: `/sales`
- Next route `/api/chat` resolves backend URL by environment:
  - `production` -> `REVIEW_BACKEND_URL_PRODUCTION`
  - `preview` -> `REVIEW_BACKEND_URL_PREVIEW`
  - fallback -> `REVIEW_BACKEND_URL`
- Vercel deploy:
  - Preview env var: `REVIEW_BACKEND_URL_PREVIEW=https://<preview-backend-domain>`
  - Production env var: `REVIEW_BACKEND_URL_PRODUCTION=https://<production-backend-domain>`
- Runtime check:
  - `GET /api/runtime` returns `deploymentEnv`, `vercelEnv`, `source`, `backendUrl`
  - UI left panel `Runtime Check` card shows the same values
  - Example: `curl https://<frontend-domain>/api/runtime`
