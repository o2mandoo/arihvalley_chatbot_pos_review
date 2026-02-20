import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .config import get_settings
from .data_store import ReviewDataStore
from .domain_router import classify_domain
from .review_service import ReviewAnalysisService
from .sales_data_store import SalesDataStore
from .sales_service import SalesAnalysisService


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


class SalesSourceUpdateRequest(BaseModel):
    report_path: Optional[str] = None
    excel_password: Optional[str] = None


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
sales_data_store: Optional[SalesDataStore] = None
sales_service: Optional[SalesAnalysisService] = None
sales_boot_error: Optional[str] = None

if settings.sales_report_path is None:
    sales_boot_error = "매출 리포트 파일(.xlsx)을 찾지 못했습니다."
else:
    try:
        sales_data_store = SalesDataStore(
            report_path=settings.sales_report_path,
            excel_password=settings.sales_excel_password,
            default_branch_name=settings.sales_branch_name,
        )
        sales_service = SalesAnalysisService(
            data_store=sales_data_store,
            openai_api_key=settings.openai_api_key,
            openai_model=settings.openai_model,
            openai_temperature=settings.openai_temperature,
            max_sql_rows=settings.max_sql_rows,
            max_table_rows=settings.max_table_rows,
        )
    except Exception as exc:  # pragma: no cover - startup diagnostics
        sales_boot_error = str(exc)

app = FastAPI(title="Review Analytics Chat Backend", version="0.1.0")
SALES_FORCE_PREFIX = "매출 분석 질문:"

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
        "sales_ready": sales_service is not None,
        "sales_report_path": str(settings.sales_report_path) if settings.sales_report_path else None,
        "sales_rows": len(sales_data_store.sales_df) if sales_data_store is not None else 0,
        "sales_sheet": sales_data_store.selected_sheet_name if sales_data_store is not None else None,
        "sales_error": sales_boot_error,
    }


def _resolve_sales_report_path(input_path: Optional[str]) -> Optional[Path]:
    if input_path is None or input_path.strip() == "":
        return settings.sales_report_path

    candidate = Path(input_path.strip()).expanduser()
    if candidate.is_absolute():
        return candidate

    project_root = Path(__file__).resolve().parents[2]
    return project_root / candidate


@app.get("/api/sales/source")
def get_sales_source() -> Dict[str, Any]:
    return {
        "ready": sales_service is not None,
        "error": sales_boot_error,
        "source": sales_data_store.source_info() if sales_data_store is not None else None,
    }


@app.post("/api/sales/source")
def update_sales_source(request: SalesSourceUpdateRequest) -> Dict[str, Any]:
    global sales_data_store, sales_service, sales_boot_error

    target_path = _resolve_sales_report_path(request.report_path)
    if target_path is None:
        raise HTTPException(status_code=400, detail="매출 리포트 파일 경로를 찾지 못했습니다.")

    if not target_path.exists():
        raise HTTPException(status_code=404, detail=f"리포트 파일이 없습니다: {target_path}")

    try:
        if sales_data_store is None:
            sales_data_store = SalesDataStore(
                report_path=target_path,
                excel_password=request.excel_password or settings.sales_excel_password,
                default_branch_name=settings.sales_branch_name,
            )
        else:
            sales_data_store.set_source(
                report_path=target_path,
                excel_password=request.excel_password,
            )

        sales_service = SalesAnalysisService(
            data_store=sales_data_store,
            openai_api_key=settings.openai_api_key,
            openai_model=settings.openai_model,
            openai_temperature=settings.openai_temperature,
            max_sql_rows=settings.max_sql_rows,
            max_table_rows=settings.max_table_rows,
        )
        sales_boot_error = None
    except Exception as exc:  # pragma: no cover - runtime update diagnostics
        sales_boot_error = str(exc)
        sales_service = None
        raise HTTPException(status_code=500, detail=f"매출 리포트 로딩 실패: {exc}") from exc

    return {
        "ready": True,
        "source": sales_data_store.source_info() if sales_data_store is not None else None,
    }


@app.post("/api/chat", response_model=ChatResponse)
def chat(request: ChatRequest) -> ChatResponse:
    start = time.perf_counter()
    raw_message = request.message.strip()
    forced_sales = False
    message = raw_message
    if raw_message.startswith(SALES_FORCE_PREFIX):
        forced_sales = True
        trimmed = raw_message[len(SALES_FORCE_PREFIX):].strip()
        if trimmed:
            message = trimmed

    domain = "sales" if forced_sales else classify_domain(message)

    if domain == "sales":
        if sales_service is None:
            latency_ms = int((time.perf_counter() - start) * 1000)
            error_message = sales_boot_error or "매출 분석 엔진 초기화에 실패했습니다."
            return ChatResponse(
                domain="sales",
                model=settings.openai_model,
                markdown=(
                    "## 매출 분석 엔진 준비 필요\n"
                    f"- 원인: {error_message}\n"
                    "- 조치: `EXCEL_PASSWORD` 설정 후 `/api/sales/source`로 리포트 재로딩"
                ),
                row_count=0,
                latency_ms=latency_ms,
            )

        try:
            answer = sales_service.answer(message)
        except Exception as exc:  # pragma: no cover - runtime error path
            raise HTTPException(status_code=500, detail=str(exc)) from exc

        latency_ms = int((time.perf_counter() - start) * 1000)
        return ChatResponse(
            domain="sales",
            model=settings.openai_model,
            sql=answer.sql,
            markdown=answer.markdown,
            row_count=answer.row_count,
            latency_ms=latency_ms,
        )

    try:
        answer = review_service.answer(message)
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
