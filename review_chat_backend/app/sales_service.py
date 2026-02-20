import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from openai import OpenAI

from .sales_data_store import SalesDataStore


FORBIDDEN_SQL = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|replace|merge|grant|revoke)\b",
    re.IGNORECASE,
)

MONEY_COLUMNS = {
    "total_sales",
    "avg_order_value",
    "product_price",
    "option_price",
    "product_discount",
    "order_discount",
    "line_total_amount",
    "actual_sales_amount",
    "net_sales_amount",
    "vat_amount",
    "총매출",
    "객단가",
    "상품가격",
    "옵션가격",
    "상품할인",
    "주문할인",
    "라인합계금액",
    "실판매금액",
    "순매출금액",
    "부가세액",
}

COUNT_COLUMNS = {
    "order_count",
    "주문 건수",
}

DATE_COLUMNS = {
    "sales_date",
    "start_date",
    "end_date",
    "매출 일자",
    "집계 시작일",
    "집계 종료일",
}


@dataclass
class SalesAnswer:
    sql: str
    markdown: str
    row_count: int


class SalesAnalysisService:
    def __init__(
        self,
        data_store: SalesDataStore,
        openai_api_key: str,
        openai_model: str,
        openai_temperature: float = 0.3,
        max_sql_rows: int = 200,
        max_table_rows: int = 20,
    ):
        self.data_store = data_store
        self.client = OpenAI(api_key=openai_api_key)
        self.model = openai_model
        self.openai_temperature = max(0.0, min(float(openai_temperature), 1.0))
        self.max_sql_rows = max_sql_rows
        self.max_table_rows = max_table_rows

    def answer(self, question: str) -> SalesAnswer:
        sql, result_df = self._run_query_with_fallback(question)
        markdown = self._build_markdown(question=question, sql=sql, query_result=result_df)
        return SalesAnswer(sql=sql, markdown=markdown, row_count=len(result_df))

    def _run_query_with_fallback(self, question: str) -> Tuple[str, pd.DataFrame]:
        fast_sql = self._fast_template_sql(question)
        if fast_sql:
            try:
                return fast_sql, self.data_store.query(fast_sql)
            except Exception:
                pass

        try:
            llm_sql = self._generate_sql(question)
            return llm_sql, self.data_store.query(llm_sql)
        except Exception:
            fallback_sql = self._fallback_sql(question)
            return fallback_sql, self.data_store.query(fallback_sql)

    @staticmethod
    def _extract_recent_days(question: str) -> Optional[int]:
        day_match = re.search(r"(\d+)\s*일", question)
        if day_match:
            try:
                days = int(day_match.group(1))
                if days > 0:
                    return min(days, 365)
            except ValueError:
                pass

        week_match = re.search(r"(\d+)\s*주", question)
        if week_match:
            try:
                weeks = int(week_match.group(1))
                if weeks > 0:
                    return min(weeks * 7, 365)
            except ValueError:
                pass

        month_match = re.search(r"(\d+)\s*(개월|달)", question)
        if month_match:
            try:
                months = int(month_match.group(1))
                if months > 0:
                    return min(months * 30, 365)
            except ValueError:
                pass

        normalized = question.replace(" ", "")
        if "일주일" in normalized:
            return 7
        if "보름" in normalized:
            return 15
        if "한달" in normalized:
            return 30
        return None

    @staticmethod
    def _extract_single_day_offset(question: str) -> Optional[int]:
        normalized = question.replace(" ", "")
        if "오늘" in normalized:
            return 0
        if "어제" in normalized:
            return 1
        if "그제" in normalized or "그저께" in normalized:
            return 2
        return None

    @staticmethod
    def _has_recent_hint(question: str) -> bool:
        normalized = question.replace(" ", "")
        return any(
            token in normalized
            for token in ("최근", "요즘", "지난", "근래", "이번주", "지난주", "이번달", "지난달", "오늘", "어제")
        )

    @staticmethod
    def _has_metric_request(question: str) -> bool:
        lowered = question.lower()
        return any(
            token in lowered
            for token in ("얼마", "몇", "개수", "건수", "count", "합계", "총", "비율", "퍼센트", "%", "평균", "순위", "언제")
        )

    @staticmethod
    def _is_daily_breakdown_intent(question: str) -> bool:
        normalized = question.replace(" ", "")
        return any(token in normalized for token in ("일자별", "날짜별", "일별", "추이", "트렌드"))

    @staticmethod
    def _is_branch_breakdown_intent(question: str) -> bool:
        normalized = question.replace(" ", "")
        return any(token in normalized for token in ("지점별", "매장별", "지점마다", "매장마다"))

    @staticmethod
    def _is_channel_breakdown_intent(question: str) -> bool:
        normalized = question.replace(" ", "")
        return "채널" in normalized

    @staticmethod
    def _is_category_breakdown_intent(question: str) -> bool:
        normalized = question.replace(" ", "")
        return "카테고리" in normalized

    @staticmethod
    def _is_day_ranking_intent(question: str) -> bool:
        normalized = question.replace(" ", "").lower()
        has_day_target = any(token in normalized for token in ("날짜", "일자", "매출일", "언제", "어느날", "무슨날", "날"))
        has_rank_target = any(
            token in normalized
            for token in ("가장", "최고", "최대", "상위", "top", "높았", "높은", "최저", "최소", "하위", "낮았", "낮은")
        )
        has_ranked_day_pattern = bool(
            re.search(r"(?:상위|하위|top|bottom)\s*\d+\s*일", normalized, flags=re.IGNORECASE)
        )
        return has_rank_target and (has_day_target or has_ranked_day_pattern)

    @staticmethod
    def _is_day_low_ranking_intent(question: str) -> bool:
        normalized = question.replace(" ", "").lower()
        return any(token in normalized for token in ("최저", "최소", "하위", "bottom", "낮았", "낮은"))

    @staticmethod
    def _extract_ranking_limit(question: str, default: int = 1) -> int:
        normalized = question.replace(" ", "")
        ranked_patterns = (
            r"(?:상위|하위|top|bottom)\s*(\d+)",
            r"(?:top|bottom)\s*[-:]?\s*(\d+)",
            r"(\d+)\s*(?:위|개)\s*(?:날|일자|날짜)?",
        )
        for pattern in ranked_patterns:
            matched = re.search(pattern, normalized, flags=re.IGNORECASE)
            if matched:
                try:
                    value = int(matched.group(1))
                    if value > 0:
                        return min(value, 30)
                except ValueError:
                    continue
        return max(1, min(default, 30))

    @staticmethod
    def _with_base_sales_cte(days: Optional[int] = None, day_offset: Optional[int] = None) -> str:
        if day_offset is not None:
            offset = max(0, day_offset)
            return f"""
WITH scoped AS (
  SELECT * FROM sales WHERE sales_date IS NOT NULL
),
latest AS (
  SELECT MAX(sales_date) AS max_date FROM scoped
),
base_sales AS (
  SELECT s.*
  FROM scoped s
  CROSS JOIN latest l
  WHERE l.max_date IS NOT NULL
    AND s.sales_date = l.max_date - INTERVAL {offset} DAY
)
""".strip()

        if days is not None:
            safe_days = max(1, min(days, 365))
            lookback_days = safe_days - 1
            return f"""
WITH scoped AS (
  SELECT * FROM sales WHERE sales_date IS NOT NULL
),
latest AS (
  SELECT MAX(sales_date) AS max_date FROM scoped
),
base_sales AS (
  SELECT s.*
  FROM scoped s
  CROSS JOIN latest l
  WHERE l.max_date IS NOT NULL
    AND s.sales_date >= l.max_date - INTERVAL {lookback_days} DAY
    AND s.sales_date <= l.max_date
)
""".strip()

        return """
WITH base_sales AS (
  SELECT * FROM sales WHERE sales_date IS NOT NULL
)
""".strip()

    def _sales_summary_sql(self, days: Optional[int] = None, day_offset: Optional[int] = None) -> str:
        with_clause = self._with_base_sales_cte(days=days, day_offset=day_offset)
        return f"""
{with_clause}
SELECT
  ROUND(SUM(net_sales_amount), 0) AS total_sales,
  COUNT(DISTINCT order_key) AS order_count,
  ROUND(SUM(net_sales_amount) / NULLIF(COUNT(DISTINCT order_key), 0), 0) AS avg_order_value,
  MIN(sales_date) AS start_date,
  MAX(sales_date) AS end_date
FROM base_sales
""".strip()

    def _order_count_sql(self, days: Optional[int] = None, day_offset: Optional[int] = None) -> str:
        with_clause = self._with_base_sales_cte(days=days, day_offset=day_offset)
        return f"""
{with_clause}
SELECT
  COUNT(DISTINCT order_key) AS order_count,
  MIN(sales_date) AS start_date,
  MAX(sales_date) AS end_date
FROM base_sales
""".strip()

    def _daily_sales_trend_sql(self, days: int = 30) -> str:
        with_clause = self._with_base_sales_cte(days=days)
        return f"""
{with_clause}
SELECT
  sales_date,
  ROUND(SUM(net_sales_amount), 0) AS total_sales,
  COUNT(DISTINCT order_key) AS order_count,
  ROUND(SUM(net_sales_amount) / NULLIF(COUNT(DISTINCT order_key), 0), 0) AS avg_order_value
FROM base_sales
GROUP BY sales_date
ORDER BY sales_date ASC
""".strip()

    def _branch_sales_sql(self, days: Optional[int] = None) -> str:
        with_clause = self._with_base_sales_cte(days=days)
        return f"""
{with_clause}
SELECT
  branch_name,
  ROUND(SUM(net_sales_amount), 0) AS total_sales,
  COUNT(DISTINCT order_key) AS order_count
FROM base_sales
GROUP BY branch_name
ORDER BY total_sales DESC
LIMIT 20
""".strip()

    def _channel_sales_sql(self, days: Optional[int] = None) -> str:
        with_clause = self._with_base_sales_cte(days=days)
        return f"""
{with_clause}
SELECT
  order_channel,
  ROUND(SUM(net_sales_amount), 0) AS total_sales,
  COUNT(DISTINCT order_key) AS order_count,
  ROUND(SUM(net_sales_amount) / NULLIF(COUNT(DISTINCT order_key), 0), 0) AS avg_order_value
FROM base_sales
GROUP BY order_channel
ORDER BY total_sales DESC
LIMIT 20
""".strip()

    def _category_sales_sql(self, days: Optional[int] = None) -> str:
        with_clause = self._with_base_sales_cte(days=days)
        return f"""
{with_clause}
SELECT
  category,
  ROUND(SUM(net_sales_amount), 0) AS total_sales,
  COUNT(DISTINCT order_key) AS order_count
FROM base_sales
GROUP BY category
ORDER BY total_sales DESC
LIMIT 20
""".strip()

    def _hourly_aov_sql(self, days: Optional[int] = None) -> str:
        with_clause = self._with_base_sales_cte(days=days)
        return f"""
{with_clause}
SELECT
  sales_hour,
  ROUND(SUM(net_sales_amount), 0) AS total_sales,
  COUNT(DISTINCT order_key) AS order_count,
  ROUND(SUM(net_sales_amount) / NULLIF(COUNT(DISTINCT order_key), 0), 0) AS avg_order_value
FROM base_sales
WHERE sales_hour IS NOT NULL
GROUP BY sales_hour
ORDER BY avg_order_value DESC, total_sales DESC
LIMIT 24
""".strip()

    def _day_sales_ranking_sql(self, days: Optional[int] = None, descending: bool = True, limit: int = 1) -> str:
        with_clause = self._with_base_sales_cte(days=days)
        order_direction = "DESC" if descending else "ASC"
        tie_direction = "DESC" if descending else "ASC"
        safe_limit = max(1, min(int(limit), 30))
        return f"""
{with_clause}
SELECT
  sales_date,
  ROUND(SUM(net_sales_amount), 0) AS total_sales,
  COUNT(DISTINCT order_key) AS order_count,
  ROUND(SUM(net_sales_amount) / NULLIF(COUNT(DISTINCT order_key), 0), 0) AS avg_order_value
FROM base_sales
GROUP BY sales_date
ORDER BY total_sales {order_direction}, sales_date {tie_direction}
LIMIT {safe_limit}
""".strip()

    @staticmethod
    def _month_compare_sql() -> str:
        return """
WITH scoped AS (
  SELECT * FROM sales WHERE sales_date IS NOT NULL
),
latest_month AS (
  SELECT DATE_TRUNC('month', MAX(sales_date)) AS current_month
  FROM scoped
),
bucketed AS (
  SELECT
    CASE
      WHEN DATE_TRUNC('month', s.sales_date) = l.current_month THEN '이번달'
      WHEN DATE_TRUNC('month', s.sales_date) = l.current_month - INTERVAL 1 MONTH THEN '지난달'
      ELSE NULL
    END AS month_bucket,
    s.net_sales_amount,
    s.order_key
  FROM scoped s
  CROSS JOIN latest_month l
)
SELECT
  month_bucket,
  ROUND(SUM(net_sales_amount), 0) AS total_sales,
  COUNT(DISTINCT order_key) AS order_count,
  ROUND(SUM(net_sales_amount) / NULLIF(COUNT(DISTINCT order_key), 0), 0) AS avg_order_value
FROM bucketed
WHERE month_bucket IS NOT NULL
GROUP BY month_bucket
ORDER BY CASE WHEN month_bucket = '지난달' THEN 1 ELSE 2 END
""".strip()

    def _fallback_sql(self, question: str) -> str:
        if self._is_day_ranking_intent(question):
            return self._day_sales_ranking_sql(
                days=30 if self._has_recent_hint(question) else None,
                descending=not self._is_day_low_ranking_intent(question),
                limit=self._extract_ranking_limit(question, default=1),
            )
        if self._is_daily_breakdown_intent(question):
            return self._daily_sales_trend_sql(days=30)
        return self._sales_summary_sql(days=30 if self._has_recent_hint(question) else None)

    def _fast_template_sql(self, question: str) -> str:
        lowered = question.lower()
        days = self._extract_recent_days(question)
        day_offset = self._extract_single_day_offset(question)
        has_recent_hint = self._has_recent_hint(question)

        has_sales_token = any(token in lowered for token in ("매출", "금액", "매상", "revenue", "sales"))
        has_order_token = any(token in lowered for token in ("주문", "order"))
        wants_count = any(token in lowered for token in ("건수", "개수", "몇", "count", "주문수", "주문 건수"))
        wants_aov = any(token in lowered for token in ("객단가", "평균 주문", "평균주문", "aov"))
        wants_month_compare = any(token in lowered for token in ("전월", "지난달", "이번달", "전 달"))

        if wants_month_compare and has_sales_token:
            return self._month_compare_sql()

        if self._is_day_ranking_intent(question):
            return self._day_sales_ranking_sql(
                days=days if (days or has_recent_hint) else None,
                descending=not self._is_day_low_ranking_intent(question),
                limit=self._extract_ranking_limit(question, default=1),
            )

        if self._is_channel_breakdown_intent(question):
            return self._channel_sales_sql(days=days if (days or has_recent_hint) else None)

        if self._is_category_breakdown_intent(question):
            return self._category_sales_sql(days=days if (days or has_recent_hint) else None)

        if self._is_branch_breakdown_intent(question):
            return self._branch_sales_sql(days=days if (days or has_recent_hint) else None)

        if self._is_daily_breakdown_intent(question):
            return self._daily_sales_trend_sql(days=days or 30)

        if wants_aov:
            return self._hourly_aov_sql(days=days or 30 if has_recent_hint else None)

        if has_order_token and wants_count and not has_sales_token:
            if day_offset is not None:
                return self._order_count_sql(days=1, day_offset=day_offset)
            if days is not None:
                return self._order_count_sql(days=days)
            return self._order_count_sql(days=30 if has_recent_hint else None)

        if has_sales_token or has_order_token:
            if day_offset is not None:
                return self._sales_summary_sql(days=1, day_offset=day_offset)
            if days is not None:
                return self._sales_summary_sql(days=days)
            if has_recent_hint:
                return self._sales_summary_sql(days=30)
            return self._sales_summary_sql()

        return ""

    def _sql_system_prompt(self) -> str:
        schema_lines: List[str] = []
        for column, dtype in self.data_store.sales_df.dtypes.items():
            schema_lines.append(f"- {column}: {dtype}")
        schema_text = "\n".join(schema_lines)

        return f"""
You are a senior analytics engineer.
Generate DuckDB SQL for a single table named sales.

Table schema:
{schema_text}

Rules:
- Return JSON only: {{"sql": "...", "reason": "..."}}
- SQL must be read-only (SELECT or WITH + SELECT only)
- Never use INSERT/UPDATE/DELETE/CREATE/DROP/ALTER/TRUNCATE
- Use sales_date for date filtering and trends
- For amount metrics, use net_sales_amount
- For order count, use COUNT(DISTINCT order_key)
- If user asks highest/lowest sales day, aggregate by sales_date and rank by total_sales.
- If result can be large, include LIMIT 100
- DuckDB syntax only
""".strip()

    def _generate_sql(self, question: str) -> str:
        response = self.client.chat.completions.create(
            model=self.model,
            temperature=self.openai_temperature,
            messages=[
                {
                    "role": "system",
                    "content": self._sql_system_prompt()
                    + "\nReturn JSON when possible. If JSON fails, return SQL only.",
                },
                {"role": "user", "content": f"사용자 질문: {question}"},
            ],
        )

        content = response.choices[0].message.content or ""
        sql = self._extract_sql_from_response(content)
        if not sql:
            raise ValueError("LLM returned empty SQL.")

        sql = self._normalize_sql(sql)
        self._validate_sql(sql)
        sql = self._ensure_safe_limit(sql)
        return sql

    @staticmethod
    def _extract_sql_from_response(content: str) -> str:
        text = content.strip()
        if not text:
            return ""

        try:
            payload = json.loads(text)
            if isinstance(payload, dict) and payload.get("sql"):
                return str(payload.get("sql", "")).strip()
        except json.JSONDecodeError:
            pass

        if "```" in text:
            for block in re.findall(r"```(?:sql)?\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL):
                candidate = block.strip()
                if candidate.lower().startswith(("select", "with")):
                    return candidate

        lowered = text.lower()
        select_pos = lowered.find("select")
        with_pos = lowered.find("with")
        starts = [pos for pos in (select_pos, with_pos) if pos != -1]
        if starts:
            return text[min(starts):].strip()
        return ""

    @staticmethod
    def _normalize_sql(sql: str) -> str:
        stripped = sql.strip()
        if stripped.startswith("```"):
            stripped = stripped.strip("`")
            stripped = stripped.replace("sql", "", 1).strip()
        return stripped.rstrip(";")

    def _validate_sql(self, sql: str) -> None:
        lowered = sql.lower().lstrip()
        if not (lowered.startswith("select") or lowered.startswith("with")):
            raise ValueError("Generated SQL must be SELECT/CTE only.")
        if FORBIDDEN_SQL.search(sql):
            raise ValueError("Unsafe SQL keyword detected.")
        if "sales" not in lowered:
            raise ValueError("SQL must reference sales table.")

    def _ensure_safe_limit(self, sql: str) -> str:
        lowered = sql.lower()
        if "limit" in lowered:
            return sql

        has_aggregate_only = bool(
            re.search(r"\b(count|avg|sum|min|max)\s*\(", lowered)
            and "group by" not in lowered
        )
        if has_aggregate_only:
            return sql
        return f"{sql}\nLIMIT {self.max_sql_rows}"

    def _build_markdown(self, question: str, sql: str, query_result: pd.DataFrame) -> str:
        if self._should_compact_answer(question, query_result):
            return self._build_compact_markdown(question=question, sql=sql, query_result=query_result)

        lines: List[str] = []
        lines.append("## 매출 분석 결과")
        lines.append(f"- 질문: {question}")
        lines.append("")

        lines.append("### 1) 핵심 결과")
        if len(query_result) == 0:
            lines.append("조회 결과가 없습니다.")
        else:
            preview = self._prepare_display_table(query_result.head(self.max_table_rows))
            lines.append(self._df_to_markdown(preview))
            if len(query_result) > self.max_table_rows:
                lines.append("")
                lines.append(f"_표시는 상위 {self.max_table_rows}행입니다. (전체 {len(query_result)}행)_")
        lines.append("")

        lines.append("### 2) 빠른 해석")
        lines.extend(self._build_insights(question, query_result))
        lines.append("")

        lines.append("```sql")
        lines.append(sql)
        lines.append("```")
        return "\n".join(lines).strip()

    def _should_compact_answer(self, question: str, query_result: pd.DataFrame) -> bool:
        if len(query_result) == 0:
            return True
        small_table = len(query_result) <= 4 and len(query_result.columns) <= 5
        return self._has_metric_request(question) and small_table

    def _build_compact_markdown(self, question: str, sql: str, query_result: pd.DataFrame) -> str:
        lines: List[str] = []
        lines.append("## 매출 분석 결과")
        lines.append(f"- 질문: {question}")
        lines.append("")
        lines.append("### 답변")
        lines.append(self._summarize_compact_answer(question, query_result))
        lines.append("")
        lines.append("### 결과 표")
        if len(query_result) == 0:
            lines.append("조회 결과가 없습니다.")
        else:
            lines.append(self._df_to_markdown(self._prepare_display_table(query_result.head(self.max_table_rows))))
        lines.append("")
        lines.append("### 빠른 해석")
        lines.extend(self._build_insights(question, query_result))
        lines.append("")
        lines.append("```sql")
        lines.append(sql)
        lines.append("```")
        return "\n".join(lines).strip()

    def _summarize_compact_answer(self, question: str, query_result: pd.DataFrame) -> str:
        if len(query_result) == 0:
            return "- 조회 결과가 없습니다."

        row = query_result.iloc[0]
        lookup = {str(column).strip().lower(): column for column in query_result.columns}

        total_sales_col = self._find_column_name(lookup, ("total_sales", "net_sales_amount", "sum"))
        order_count_col = self._find_column_name(lookup, ("order_count", "count"))
        aov_col = self._find_column_name(lookup, ("avg_order_value", "avg"))
        sales_date_col = self._find_column_name(lookup, ("sales_date", "date"))
        start_col = self._find_column_name(lookup, ("start_date", "min_date", "sales_date"))
        end_col = self._find_column_name(lookup, ("end_date", "max_date"))

        parts: List[str] = []
        if sales_date_col is not None:
            sales_date_text = self._format_date(row[sales_date_col])
            if sales_date_text:
                parts.append(f"매출일은 **{sales_date_text}**")

        if total_sales_col is not None:
            value = pd.to_numeric(row[total_sales_col], errors="coerce")
            if value is not None and not pd.isna(value):
                parts.append(f"매출은 **{int(value):,}원**")

        if order_count_col is not None:
            value = pd.to_numeric(row[order_count_col], errors="coerce")
            if value is not None and not pd.isna(value):
                parts.append(f"주문 건수는 **{int(value):,}건**")

        if aov_col is not None:
            value = pd.to_numeric(row[aov_col], errors="coerce")
            if value is not None and not pd.isna(value):
                parts.append(f"객단가는 **{int(value):,}원**")

        if not parts:
            return "- 요청하신 결과를 표로 정리했습니다."

        period = ""
        if start_col is not None and end_col is not None:
            start_text = self._format_date(row[start_col])
            end_text = self._format_date(row[end_col])
            if start_text and end_text:
                period = f" (기간: {start_text} ~ {end_text})"

        context = ""
        day_offset = self._extract_single_day_offset(question)
        days = self._extract_recent_days(question)
        if day_offset == 0:
            context = "오늘 기준 "
        elif day_offset == 1:
            context = "어제 기준 "
        elif day_offset == 2:
            context = "그저께 기준 "
        elif days:
            context = f"최근 {days}일 기준 "
        elif self._has_recent_hint(question):
            context = "최근 기준 "

        return f"- {context}{', '.join(parts)}입니다.{period}"

    def _build_insights(self, question: str, query_result: pd.DataFrame) -> List[str]:
        if len(query_result) == 0:
            return ["- 결과가 없어 해석 포인트를 계산하지 못했습니다."]

        bullets: List[str] = []
        lookup = {str(column).strip().lower(): column for column in query_result.columns}
        total_sales_col = self._find_column_name(lookup, ("total_sales",))
        order_count_col = self._find_column_name(lookup, ("order_count",))
        aov_col = self._find_column_name(lookup, ("avg_order_value",))

        if len(query_result) == 1 and total_sales_col is not None:
            total_sales = pd.to_numeric(query_result.iloc[0][total_sales_col], errors="coerce")
            if total_sales is not None and not pd.isna(total_sales):
                bullets.append(f"- 집계 매출은 **{self._format_currency(total_sales)}**입니다.")
        if len(query_result) == 1 and order_count_col is not None:
            order_count = pd.to_numeric(query_result.iloc[0][order_count_col], errors="coerce")
            if order_count is not None and not pd.isna(order_count):
                bullets.append(f"- 주문 건수는 **{self._format_count(order_count)}**입니다.")
        if len(query_result) == 1 and aov_col is not None:
            aov = pd.to_numeric(query_result.iloc[0][aov_col], errors="coerce")
            if aov is not None and not pd.isna(aov):
                bullets.append(f"- 평균 객단가는 **{self._format_currency(aov)}**입니다.")

        if len(query_result) > 1 and total_sales_col is not None:
            numeric = pd.to_numeric(query_result[total_sales_col], errors="coerce").fillna(0)
            top_idx = numeric.idxmax()
            top_val = numeric.loc[top_idx]
            label_col = next((column for column in query_result.columns if column != total_sales_col), None)
            if label_col is not None:
                label = self._format_dimension_label(query_result.loc[top_idx, label_col])
                bullets.append(f"- 가장 높은 매출 구간은 **{label} ({self._format_currency(top_val)})**입니다.")

        bullets.extend(self._build_period_comparison_insights(question, query_result))

        if not bullets:
            bullets.append("- 표 데이터를 기반으로 추가 해석을 진행하려면 비교 기준(기간/채널/카테고리)을 지정해 주세요.")
        return bullets

    def _build_period_comparison_insights(self, question: str, query_result: pd.DataFrame) -> List[str]:
        days = self._extract_recent_days(question)
        if days is None and self._has_recent_hint(question):
            days = self._infer_days_from_result_range(query_result)
        if days is None or days <= 0:
            return []

        try:
            summary = self._fetch_period_summary(days)
        except Exception:
            return []

        current_sales = float(summary.get("current_total_sales", 0.0) or 0.0)
        previous_sales = float(summary.get("previous_total_sales", 0.0) or 0.0)
        current_orders = float(summary.get("current_order_count", 0.0) or 0.0)
        previous_orders = float(summary.get("previous_order_count", 0.0) or 0.0)
        current_aov = float(summary.get("current_aov", 0.0) or 0.0)
        previous_aov = float(summary.get("previous_aov", 0.0) or 0.0)

        sales_delta = current_sales - previous_sales
        orders_delta = current_orders - previous_orders
        aov_delta = current_aov - previous_aov

        sales_pct = self._pct_delta(current_sales, previous_sales)
        orders_pct = self._pct_delta(current_orders, previous_orders)
        aov_pct = self._pct_delta(current_aov, previous_aov)

        lines: List[str] = []
        lines.append(
            (
                "- 최근 {days}일은 직전 {days}일 대비 "
                "매출 **{sales_delta} ({sales_pct})**, 주문 **{orders_delta} ({orders_pct})**, "
                "객단가 **{aov_delta} ({aov_pct})** 변동입니다."
            ).format(
                days=days,
                sales_delta=self._format_signed_currency(sales_delta),
                sales_pct=self._format_signed_percent(sales_pct),
                orders_delta=self._format_signed_count(orders_delta),
                orders_pct=self._format_signed_percent(orders_pct),
                aov_delta=self._format_signed_currency(aov_delta),
                aov_pct=self._format_signed_percent(aov_pct),
            )
        )

        order_direction = "증가" if orders_delta > 0 else "감소" if orders_delta < 0 else "보합"
        aov_direction = "증가" if aov_delta > 0 else "감소" if aov_delta < 0 else "보합"
        if sales_delta > 0 and aov_delta > 0 and orders_delta <= 0:
            lines.append("- 매출 상승은 주문량보다 **객단가 상승 영향**이 더 크게 작용한 패턴입니다.")
        elif sales_delta > 0 and orders_delta > 0 and aov_delta <= 0:
            lines.append("- 매출 상승은 고가 판매보다 **주문량 증가**가 주도한 패턴입니다.")
        elif sales_delta < 0 and orders_delta < 0 and aov_delta >= 0:
            lines.append("- 매출 하락의 주된 원인은 **방문/주문 건수 감소**로 해석됩니다.")
        elif sales_delta < 0 and aov_delta < 0 and orders_delta >= 0:
            lines.append("- 주문 수는 유지됐지만 **객단가 하락**이 매출 감소에 영향을 준 흐름입니다.")
        else:
            lines.append(f"- 현재 변동은 주문({order_direction})과 객단가({aov_direction})가 혼합되어 나타난 패턴입니다.")

        channel_line = self._build_driver_insight(days=days, dimension_column="order_channel", dimension_name="주문채널")
        if channel_line:
            lines.append(channel_line)

        category_line = self._build_driver_insight(days=days, dimension_column="category", dimension_name="카테고리")
        if category_line:
            lines.append(category_line)

        return lines

    def _fetch_period_summary(self, days: int) -> Dict[str, float]:
        safe_days = max(1, min(int(days), 365))
        lookback = safe_days - 1
        prev_start = safe_days * 2 - 1
        prev_end = safe_days
        sql = f"""
WITH scoped AS (
  SELECT sales_date, net_sales_amount, order_key
  FROM sales
  WHERE sales_date IS NOT NULL
),
latest AS (
  SELECT MAX(sales_date) AS max_date FROM scoped
),
current_window AS (
  SELECT s.*
  FROM scoped s
  CROSS JOIN latest l
  WHERE l.max_date IS NOT NULL
    AND s.sales_date >= l.max_date - INTERVAL {lookback} DAY
    AND s.sales_date <= l.max_date
),
previous_window AS (
  SELECT s.*
  FROM scoped s
  CROSS JOIN latest l
  WHERE l.max_date IS NOT NULL
    AND s.sales_date >= l.max_date - INTERVAL {prev_start} DAY
    AND s.sales_date <= l.max_date - INTERVAL {prev_end} DAY
)
SELECT
  COALESCE((SELECT SUM(net_sales_amount) FROM current_window), 0) AS current_total_sales,
  COALESCE((SELECT COUNT(DISTINCT order_key) FROM current_window), 0) AS current_order_count,
  COALESCE(
    (SELECT SUM(net_sales_amount) / NULLIF(COUNT(DISTINCT order_key), 0) FROM current_window),
    0
  ) AS current_aov,
  COALESCE((SELECT SUM(net_sales_amount) FROM previous_window), 0) AS previous_total_sales,
  COALESCE((SELECT COUNT(DISTINCT order_key) FROM previous_window), 0) AS previous_order_count,
  COALESCE(
    (SELECT SUM(net_sales_amount) / NULLIF(COUNT(DISTINCT order_key), 0) FROM previous_window),
    0
  ) AS previous_aov
""".strip()
        result = self.data_store.query(sql)
        if len(result) == 0:
            return {}
        return {
            "current_total_sales": float(result.iloc[0]["current_total_sales"] or 0.0),
            "current_order_count": float(result.iloc[0]["current_order_count"] or 0.0),
            "current_aov": float(result.iloc[0]["current_aov"] or 0.0),
            "previous_total_sales": float(result.iloc[0]["previous_total_sales"] or 0.0),
            "previous_order_count": float(result.iloc[0]["previous_order_count"] or 0.0),
            "previous_aov": float(result.iloc[0]["previous_aov"] or 0.0),
        }

    def _build_driver_insight(self, days: int, dimension_column: str, dimension_name: str) -> str:
        driver_df = self._fetch_driver_delta(days=days, dimension_column=dimension_column)
        if len(driver_df) == 0:
            return ""

        positive = driver_df[driver_df["delta_sales"] > 0]
        negative = driver_df[driver_df["delta_sales"] < 0]
        pieces: List[str] = []

        if len(positive) > 0:
            top_up = positive.sort_values("delta_sales", ascending=False).iloc[0]
            pieces.append(
                f"증가 기여는 **{top_up['dimension']} ({self._format_signed_currency(top_up['delta_sales'])})**"
            )

        if len(negative) > 0:
            top_down = negative.sort_values("delta_sales", ascending=True).iloc[0]
            pieces.append(
                f"감소 기여는 **{top_down['dimension']} ({self._format_signed_currency(top_down['delta_sales'])})**"
            )

        if not pieces:
            return ""
        return f"- 직전 {days}일 대비 {dimension_name} 기준으로는 " + ", ".join(pieces) + "입니다."

    def _fetch_driver_delta(self, days: int, dimension_column: str) -> pd.DataFrame:
        safe_days = max(1, min(int(days), 365))
        lookback = safe_days - 1
        prev_start = safe_days * 2 - 1
        prev_end = safe_days
        sql = f"""
WITH scoped AS (
  SELECT sales_date, net_sales_amount, {dimension_column} AS dimension
  FROM sales
  WHERE sales_date IS NOT NULL
),
latest AS (
  SELECT MAX(sales_date) AS max_date FROM scoped
),
windowed AS (
  SELECT
    s.dimension,
    CASE
      WHEN s.sales_date >= l.max_date - INTERVAL {lookback} DAY
       AND s.sales_date <= l.max_date THEN 'current'
      WHEN s.sales_date >= l.max_date - INTERVAL {prev_start} DAY
       AND s.sales_date <= l.max_date - INTERVAL {prev_end} DAY THEN 'previous'
      ELSE NULL
    END AS period,
    s.net_sales_amount
  FROM scoped s
  CROSS JOIN latest l
),
agg AS (
  SELECT
    dimension,
    period,
    SUM(net_sales_amount) AS total_sales
  FROM windowed
  WHERE period IS NOT NULL
    AND COALESCE(TRIM(dimension), '') <> ''
  GROUP BY dimension, period
),
pivoted AS (
  SELECT
    COALESCE(c.dimension, p.dimension) AS dimension,
    COALESCE(c.total_sales, 0) AS current_sales,
    COALESCE(p.total_sales, 0) AS previous_sales
  FROM (SELECT dimension, total_sales FROM agg WHERE period = 'current') c
  FULL OUTER JOIN (SELECT dimension, total_sales FROM agg WHERE period = 'previous') p
    ON c.dimension = p.dimension
)
SELECT
  dimension,
  current_sales,
  previous_sales,
  current_sales - previous_sales AS delta_sales
FROM pivoted
ORDER BY ABS(current_sales - previous_sales) DESC
LIMIT 6
""".strip()
        return self.data_store.query(sql)

    @staticmethod
    def _pct_delta(current: float, previous: float) -> Optional[float]:
        if previous == 0:
            if current == 0:
                return 0.0
            return None
        return ((current - previous) / previous) * 100.0

    @staticmethod
    def _infer_days_from_result_range(query_result: pd.DataFrame) -> Optional[int]:
        if len(query_result) == 0:
            return None
        lookup = {str(column).strip().lower(): column for column in query_result.columns}
        start_col = SalesAnalysisService._find_column_name(lookup, ("start_date",))
        end_col = SalesAnalysisService._find_column_name(lookup, ("end_date",))
        if start_col is None or end_col is None:
            return None
        start = pd.to_datetime(query_result.iloc[0][start_col], errors="coerce")
        end = pd.to_datetime(query_result.iloc[0][end_col], errors="coerce")
        if pd.isna(start) or pd.isna(end):
            return None
        delta_days = int((end - start).days) + 1
        if delta_days <= 0:
            return None
        return min(delta_days, 365)

    def _prepare_display_table(self, df: pd.DataFrame) -> pd.DataFrame:
        localized = self._localize_columns(df)
        rendered = localized.copy()
        for column in rendered.columns:
            rendered[column] = rendered[column].map(lambda value: self._format_display_cell(str(column), value))
        return rendered

    def _format_display_cell(self, column_name: str, value: Any) -> str:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return ""

        normalized_column = column_name.strip().lower()
        original_column = column_name.strip()

        if self._is_date_like_value(value, normalized_column, original_column):
            return self._format_date(value)

        if self._is_percent_column(normalized_column, original_column):
            numeric = pd.to_numeric(value, errors="coerce")
            if pd.isna(numeric):
                return str(value)
            return f"{float(numeric):.1f}%"

        numeric = pd.to_numeric(value, errors="coerce")
        if pd.isna(numeric):
            return self._clip_cell(value)

        if self._is_money_column(normalized_column, original_column):
            return self._format_currency(numeric)

        if self._is_count_column(normalized_column, original_column):
            return self._format_count(numeric)

        if float(numeric).is_integer():
            return f"{int(numeric):,}"
        return f"{float(numeric):,.1f}"

    @staticmethod
    def _is_percent_column(normalized_column: str, original_column: str) -> bool:
        return (
            "ratio" in normalized_column
            or "pct" in normalized_column
            or "비율" in original_column
            or "%" in original_column
        )

    @staticmethod
    def _is_money_column(normalized_column: str, original_column: str) -> bool:
        return normalized_column in {column.lower() for column in MONEY_COLUMNS} or original_column in MONEY_COLUMNS

    @staticmethod
    def _is_count_column(normalized_column: str, original_column: str) -> bool:
        return (
            normalized_column in {column.lower() for column in COUNT_COLUMNS}
            or original_column in COUNT_COLUMNS
            or normalized_column.endswith("_count")
            or "건수" in original_column
        )

    @staticmethod
    def _is_date_like_value(value: Any, normalized_column: str, original_column: str) -> bool:
        if normalized_column in {column.lower() for column in DATE_COLUMNS} or original_column in DATE_COLUMNS:
            return True
        if "date" in normalized_column or "일자" in original_column:
            return True
        if isinstance(value, (pd.Timestamp,)):
            return True
        if hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day"):
            return True
        if isinstance(value, str):
            normalized = value.strip()
            if re.match(r"^\d{4}-\d{2}-\d{2}", normalized):
                return True
        return False

    @staticmethod
    def _format_currency(value: Any) -> str:
        numeric = pd.to_numeric(value, errors="coerce")
        if pd.isna(numeric):
            return "0원"
        return f"{int(round(float(numeric))):,}원"

    @staticmethod
    def _format_count(value: Any) -> str:
        numeric = pd.to_numeric(value, errors="coerce")
        if pd.isna(numeric):
            return "0건"
        return f"{int(round(float(numeric))):,}건"

    @staticmethod
    def _format_signed_currency(value: Any) -> str:
        numeric = pd.to_numeric(value, errors="coerce")
        if pd.isna(numeric):
            return "0원"
        signed = int(round(float(numeric)))
        if signed > 0:
            return f"+{signed:,}원"
        return f"{signed:,}원"

    @staticmethod
    def _format_signed_count(value: Any) -> str:
        numeric = pd.to_numeric(value, errors="coerce")
        if pd.isna(numeric):
            return "0건"
        signed = int(round(float(numeric)))
        if signed > 0:
            return f"+{signed:,}건"
        return f"{signed:,}건"

    @staticmethod
    def _format_signed_percent(value: Optional[float]) -> str:
        if value is None:
            return "신규 구간"
        if value > 0:
            return f"+{value:.1f}%"
        return f"{value:.1f}%"

    def _format_dimension_label(self, value: Any) -> str:
        if self._is_date_like_value(value, "", ""):
            return self._format_date(value)
        return self._clip_cell(value)

    @staticmethod
    def _find_column_name(lookup: Dict[str, object], candidates: Tuple[str, ...]) -> Optional[object]:
        for candidate in candidates:
            found = lookup.get(candidate.lower())
            if found is not None:
                return found
        return None

    @staticmethod
    def _format_date(value: object) -> str:
        if value is None or pd.isna(value):
            return ""
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return str(value)
        return parsed.strftime("%Y-%m-%d")

    @staticmethod
    def _localize_columns(df: pd.DataFrame) -> pd.DataFrame:
        mapping = {
            "sales_row_id": "행 번호",
            "report_name": "리포트 파일",
            "sheet_name": "시트명",
            "branch_name": "지점명",
            "order_base_date": "주문 기준 일시",
            "order_start_time": "주문 시작 시각",
            "sales_date": "매출 일자",
            "sales_year": "매출 연도",
            "sales_month": "매출 월",
            "sales_day": "매출 일",
            "sales_hour": "매출 시각(시)",
            "order_number": "주문번호",
            "order_key": "주문 식별키",
            "order_channel": "주문채널",
            "payment_status": "결제상태",
            "category": "카테고리",
            "product_name": "상품명",
            "option_name": "옵션명",
            "tax_type": "과세여부",
            "quantity": "수량",
            "product_price": "상품가격",
            "option_price": "옵션가격",
            "product_discount": "상품할인",
            "order_discount": "주문할인",
            "line_total_amount": "라인합계금액",
            "actual_sales_amount": "실판매금액",
            "net_sales_amount": "순매출금액",
            "vat_amount": "부가세액",
            "total_sales": "총매출",
            "order_count": "주문 건수",
            "avg_order_value": "객단가",
            "start_date": "집계 시작일",
            "end_date": "집계 종료일",
        }

        renamed = {
            column: mapping.get(str(column).strip().lower(), str(column))
            for column in df.columns
        }
        return df.rename(columns=renamed)

    @staticmethod
    def _df_to_markdown(df: pd.DataFrame) -> str:
        if len(df.columns) == 0:
            return "(empty table)"

        headers = [str(col) for col in df.columns]
        rows = [headers]
        rows += [[SalesAnalysisService._clip_cell(v) for v in row] for row in df.values.tolist()]

        widths = [0] * len(headers)
        for row in rows:
            for idx, cell in enumerate(row):
                widths[idx] = max(widths[idx], len(cell))

        def fmt(row: List[str]) -> str:
            return "| " + " | ".join(cell.ljust(widths[idx]) for idx, cell in enumerate(row)) + " |"

        divider = "| " + " | ".join("-" * widths[idx] for idx in range(len(headers))) + " |"
        lines = [fmt(rows[0]), divider]
        lines.extend(fmt(row) for row in rows[1:])
        return "\n".join(lines)

    @staticmethod
    def _clip_cell(value: object, max_len: int = 180) -> str:
        if pd.isna(value):
            return ""
        text = str(value)
        text = re.sub(r"\s+", " ", text).strip()
        if len(text) > max_len:
            return text[: max_len - 3] + "..."
        return text
