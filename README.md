# fms-platform

Franchise Management System platform workspace.

## What is in this repo

- `scripts/pipeline/`: sales Excel ingestion pipeline (decrypt -> transform -> import to PostgreSQL)
- `prisma/`: normalized DB schema + migrations + `sales_records` analytics view
- `chatbi_nl2sql/`: NL2SQL chatbot for `sales_records`
- `review_analysis/`: Naver review crawling + LLM analysis pipeline

## Quick start

The fastest way to run from a clean environment is documented in `RUNBOOK.md`.

- Recommended first run: sales data pipeline (`scripts/pipeline/run-pipeline.js`)
- Then optional: NL2SQL (`chatbi_nl2sql/chatbi_nl2sql.py`)
- Then optional: review analysis (`review_analysis/run_pipeline.py`)
- Web demo (review chatbot): `review_chat_backend/` + `review_chat_frontend/` (Next.js, Vercel-ready)
