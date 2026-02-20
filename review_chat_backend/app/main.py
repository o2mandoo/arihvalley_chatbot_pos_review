import time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .config import get_settings
from .data_store import ReviewDataStore
from .domain_router import classify_domain
from .review_service import ReviewAnalysisService


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1)
    history: List[Dict[str, Any]] = Field(default_factory=list)


class ChatResponse(BaseModel):
    domain: str
    model: str
    sql: Optional[str] = None
    markdown: str
    row_count: int = 0
    latency_ms: int


settings = get_settings()
data_store = ReviewDataStore(settings.review_csv_path)
review_service = ReviewAnalysisService(
    data_store=data_store,
    openai_api_key=settings.openai_api_key,
    openai_model=settings.openai_model,
    openai_temperature=settings.openai_temperature,
    max_sql_rows=settings.max_sql_rows,
    max_table_rows=settings.max_table_rows,
)

app = FastAPI(title="Review Analytics Chat Backend", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "model": settings.openai_model,
        "temperature": settings.openai_temperature,
        "review_csv": str(settings.review_csv_path),
        "rows": len(data_store.reviews_df),
    }


@app.post("/api/chat", response_model=ChatResponse)
def chat(request: ChatRequest) -> ChatResponse:
    start = time.perf_counter()
    domain = classify_domain(request.message)

    if domain == "sales":
        latency_ms = int((time.perf_counter() - start) * 1000)
        return ChatResponse(
            domain="sales",
            model=settings.openai_model,
            markdown=(
                "## 매출 분석 요청으로 분류됨\n"
                "현재 데모는 리뷰 분석에 집중되어 있어 매출 분석 엔진은 아직 연결되지 않았습니다.\n\n"
                "- 상태: `planned`\n"
                "- 현재 사용 가능한 경로: 리뷰 분석 질의"
            ),
            row_count=0,
            latency_ms=latency_ms,
        )

    try:
        answer = review_service.answer(request.message)
    except Exception as exc:  # pragma: no cover - runtime error path
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    latency_ms = int((time.perf_counter() - start) * 1000)
    return ChatResponse(
        domain="review",
        model=settings.openai_model,
        sql=answer.sql,
        markdown=answer.markdown,
        row_count=answer.row_count,
        latency_ms=latency_ms,
    )
