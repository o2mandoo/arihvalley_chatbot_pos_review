import json
import random
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd
from openai import OpenAI

from .data_store import ReviewDataStore


SQL_SYSTEM_PROMPT = """
You are a senior analytics engineer.
Generate DuckDB SQL for a single table named reviews.

Table schema:
- review_id BIGINT
- branch_name VARCHAR
- date_text VARCHAR (Korean date text)
- review_date DATE
- review_year BIGINT
- review_month BIGINT
- review_day BIGINT
- nickname VARCHAR
- review_content VARCHAR
- wait_time VARCHAR
- review_length BIGINT

Rules:
- Return JSON only: {"sql": "...", "reason": "..."}
- SQL must be read-only (SELECT or WITH + SELECT only)
- Never use INSERT/UPDATE/DELETE/CREATE/DROP/ALTER/TRUNCATE
- Use review_date for time filters
- Use ILIKE for Korean keyword search
- If user asks for examples, include review_content and branch_name
- If user asks for ranking/top, sort DESC and include LIMIT
- If query may return many rows, include LIMIT 50
- Use DuckDB-compatible SQL functions only.
- Do not use array_join. Use string_agg/listagg style aggregation instead.
""".strip()


NEGATIVE_SIGNAL_PATTERNS: Dict[str, str] = {
    "웨이팅/대기": r"웨이팅|대기|기다리",
    "혼잡/소음": r"시끄럽|복잡|혼잡|사람\s*많",
    "서비스 속도": r"늦|느리|오래\s*걸",
    "서비스 태도": r"불친절|응대\s*별로|서비스\s*별로",
    "공간/좌석": r"좁|자리\s*없|좌석",
    "가격/가성비 불만": r"비싸|가격\s*부담|가성비\s*별로",
    "맛 디테일 불만": r"짜|싱겁|아쉽|별로|물리",
}

POSITIVE_HINT = re.compile(r"맛있|좋|친절|추천|만족|훌륭|재방문", re.IGNORECASE)
NEGATIVE_HINT = re.compile(r"근데|하지만|다만|아쉽|별로|시끄럽|웨이팅|좁|불친절|늦", re.IGNORECASE)
REVISIT_INTENT_HINT = re.compile(
    r"재방문|또\s*갈|또\s*오|다시\s*방문|다시\s*올|또\s*방문",
    re.IGNORECASE,
)
EMAIL_PATTERN = re.compile(
    r"([A-Za-z0-9._%+-])[A-Za-z0-9._%+-]*@([A-Za-z0-9.-]+\.[A-Za-z]{2,})"
)
PHONE_PATTERN = re.compile(r"(?<!\d)(01[016789]|02|0[3-9]\d)[-\s]?\d{3,4}[-\s]?\d{4}(?!\d)")
LONG_DIGIT_PATTERN = re.compile(r"(?<!\d)\d{6,}(?!\d)")
HANDLE_PATTERN = re.compile(r"@([A-Za-z0-9._-]{3,})")
NAME_WITH_NIM_PATTERN = re.compile(r"([가-힣A-Za-z0-9]{2,10})님")
NEGATIVE_ANY_PATTERN = re.compile(
    "|".join(f"(?:{pattern})" for pattern in NEGATIVE_SIGNAL_PATTERNS.values()),
    re.IGNORECASE,
)
WAITING_SQL_REGEX = r"웨이팅|대기|기다|줄"
NEGATIVE_ANY_SQL_REGEX = (
    r"웨이팅|대기|기다리|시끄럽|복잡|혼잡|사람\s*많|늦|느리|오래\s*걸|불친절|응대\s*별로|서비스\s*별로|"
    r"좁|자리\s*없|좌석|비싸|가격\s*부담|가성비\s*별로|짜|싱겁|아쉽|별로|물리"
)

FORBIDDEN_SQL = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|replace|merge|grant|revoke)\b",
    re.IGNORECASE,
)

UNSUPPORTED_FUNCTION_PATTERNS = [
    re.compile(r"\barray_join\s*\(", re.IGNORECASE),
]


@dataclass
class ReviewAnswer:
    sql: str
    markdown: str
    row_count: int
    scope: str


class ReviewAnalysisService:
    def __init__(
        self,
        data_store: ReviewDataStore,
        openai_api_key: str,
        openai_model: str,
        openai_temperature: float = 0.35,
        max_sql_rows: int = 200,
        max_table_rows: int = 20,
    ):
        self.data_store = data_store
        self.client = OpenAI(api_key=openai_api_key)
        self.model = openai_model
        self.openai_temperature = max(0.0, min(float(openai_temperature), 1.0))
        self.answer_variation_temperature = min(1.0, self.openai_temperature * 2.0 + 0.1)
        self._rng = random.SystemRandom()
        self.max_sql_rows = max_sql_rows
        self.max_table_rows = max_table_rows

    def answer(self, question: str) -> ReviewAnswer:
        sql, result_df = self._run_query_with_fallback(question)

        scope_df, scope_label = self._select_scope(question)
        signal_df = self._compute_negative_signals(scope_df)
        hidden_examples = self._find_hidden_negatives(scope_df)

        markdown = self._build_markdown(
            question=question,
            sql=sql,
            query_result=result_df,
            signal_df=signal_df,
            hidden_examples=hidden_examples,
            scope_label=scope_label,
            scope_df=scope_df,
        )
        return ReviewAnswer(
            sql=sql,
            markdown=markdown,
            row_count=len(result_df),
            scope=scope_label,
        )

    def _run_query_with_fallback(self, question: str) -> Tuple[str, pd.DataFrame]:
        fast_sql = self._fast_template_sql(question)
        where_clause = self._branch_where_clause(question)

        if fast_sql:
            try:
                return fast_sql, self.data_store.query(fast_sql)
            except Exception:
                safe_sql = self._fallback_sql(question)
                if safe_sql.strip() == fast_sql.strip():
                    raise
                return safe_sql, self.data_store.query(safe_sql)

        try:
            llm_sql = self._generate_sql(question)
            return llm_sql, self.data_store.query(llm_sql)
        except Exception:
            fallback_sql = self._fallback_sql(question)
            return fallback_sql, self.data_store.query(fallback_sql)

    def _generate_sql_with_fast_path(self, question: str) -> str:
        fast_sql = self._fast_template_sql(question)
        if fast_sql:
            return fast_sql
        return self._generate_sql(question)

    def _fast_template_sql(self, question: str) -> str:
        q = question.lower()
        where_clause = self._branch_where_clause(question)

        metric_sql = self._metric_template_sql(question, where_clause)
        if metric_sql:
            return metric_sql

        if "숨은" in question and "불만" in question:
            return f"""
WITH scoped AS (
  SELECT * FROM reviews
  WHERE {where_clause}
),
target AS (
  SELECT
    review_date,
    branch_name,
    nickname,
    review_content
  FROM scoped
  WHERE regexp_matches(review_content, '맛있|좋|친절|추천|만족|훌륭|재방문', 'i')
    AND regexp_matches(review_content, '근데|하지만|다만|아쉽|별로|시끄럽|웨이팅|좁|불친절|늦', 'i')
)
SELECT *
FROM target
ORDER BY review_date DESC
LIMIT 12
""".strip()

        if self._is_negative_signal_intent(question):
            return self._negative_signal_sql(where_clause)

        waiting_tokens = ["웨이팅", "대기", "기다", "줄"]
        if any(token in q for token in waiting_tokens):
            return f"""
WITH scoped AS (
  SELECT * FROM reviews
  WHERE {where_clause}
),
waiting AS (
  SELECT
    review_date,
    branch_name,
    nickname,
    review_content
  FROM scoped
  WHERE regexp_matches(review_content, '웨이팅|대기|기다|줄', 'i')
)
SELECT
  (SELECT COUNT(*) FROM waiting) AS waiting_count,
  ROUND(
    100.0 * (SELECT COUNT(*) FROM waiting) / NULLIF((SELECT COUNT(*) FROM scoped), 0),
    1
  ) AS ratio_pct,
  review_date,
  branch_name,
  nickname,
  review_content
FROM waiting
ORDER BY review_date DESC
LIMIT 8
""".strip()

        return ""

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
        if "한달" in normalized or "한달간" in normalized:
            return 30
        if "한달반" in normalized:
            return 45
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
            for token in (
                "최근",
                "요즘",
                "지난",
                "근래",
                "이번주",
                "저번주",
                "이번달",
                "지난달",
                "오늘",
                "어제",
                "그저께",
            )
        )

    @staticmethod
    def _has_metric_request(question: str) -> bool:
        lowered = question.lower()
        return any(
            token in lowered
            for token in (
                "몇개",
                "몇 개",
                "개수",
                "몇건",
                "몇 건",
                "건수",
                "count",
                "얼마",
                "총",
                "합계",
                "비율",
                "퍼센트",
                "%",
                "비중",
            )
        )

    def _is_review_metric_intent(self, question: str) -> bool:
        has_review = any(token in question for token in ("리뷰", "후기"))
        has_structure_intent = self._is_daily_breakdown_intent(question) or self._is_branch_breakdown_intent(question)
        return has_review and (self._has_metric_request(question) or has_structure_intent)

    def _is_waiting_metric_intent(self, question: str) -> bool:
        lowered = question.lower()
        waiting_tokens = ("웨이팅", "대기", "기다", "줄")
        has_structure_intent = self._is_daily_breakdown_intent(question) or self._is_branch_breakdown_intent(question)
        return any(token in lowered for token in waiting_tokens) and (
            self._has_metric_request(question) or has_structure_intent
        )

    def _is_negative_metric_intent(self, question: str) -> bool:
        lowered = question.lower()
        negative_tokens = ("부정", "불만", "아쉬", "문제", "불편", "컴플레인")
        has_structure_intent = self._is_daily_breakdown_intent(question) or self._is_branch_breakdown_intent(question)
        return any(token in lowered for token in negative_tokens) and (
            self._has_metric_request(question) or has_structure_intent
        )

    @staticmethod
    def _is_branch_breakdown_intent(question: str) -> bool:
        normalized = question.replace(" ", "")
        return any(token in normalized for token in ("지점별", "매장별", "지점마다", "매장마다"))

    @staticmethod
    def _is_daily_breakdown_intent(question: str) -> bool:
        normalized = question.replace(" ", "")
        return any(token in normalized for token in ("일자별", "날짜별", "하루별", "일별", "추이", "트렌드"))

    def _is_simple_review_count_intent(self, question: str) -> bool:
        lowered = question.lower()
        review_tokens = ("리뷰", "후기")
        count_tokens = ("몇개", "몇 개", "개수", "몇건", "몇 건", "건수", "count", "얼마", "총")
        blockers = (
            "지점별",
            "추이",
            "비교",
            "패턴",
            "불만",
            "신호",
            "인사이트",
            "원인",
            "예시",
            "목록",
            "top",
            "순위",
            "분석",
        )

        if any(token in lowered for token in blockers):
            return False

        has_review = any(token in question for token in review_tokens)
        has_count = (
            any(token in lowered for token in count_tokens)
            or "리뷰수" in question
            or "리뷰 개수" in question
        )
        has_recent = self._has_recent_hint(question) or (self._extract_recent_days(question) is not None)
        return has_review and has_count and has_recent

    def _metric_template_sql(self, question: str, where_clause: str) -> str:
        has_waiting_metric = self._is_waiting_metric_intent(question)
        has_negative_metric = self._is_negative_metric_intent(question)
        has_review_metric = self._is_review_metric_intent(question)

        if not (has_review_metric or has_waiting_metric or has_negative_metric):
            return ""

        days = self._extract_recent_days(question)
        day_offset = self._extract_single_day_offset(question)
        has_recent_hint = self._has_recent_hint(question)
        is_branch = self._is_branch_breakdown_intent(question)
        is_daily = self._is_daily_breakdown_intent(question)

        if has_waiting_metric:
            if is_daily:
                return self._waiting_by_day_sql(where_clause, days or 30)
            if is_branch:
                return self._waiting_by_branch_sql(where_clause, days if (days or has_recent_hint) else None)
            if day_offset is not None:
                return self._waiting_metric_sql(where_clause, days=1, day_offset=day_offset)
            return self._waiting_metric_sql(where_clause, days=(days or 30) if has_recent_hint or days else None)

        if has_negative_metric:
            if is_daily:
                return self._negative_by_day_sql(where_clause, days or 30)
            if is_branch:
                return self._negative_by_branch_sql(where_clause, days if (days or has_recent_hint) else None)
            if day_offset is not None:
                return self._negative_metric_sql(where_clause, days=1, day_offset=day_offset)
            return self._negative_metric_sql(where_clause, days=(days or 30) if has_recent_hint or days else None)

        if has_review_metric:
            if is_daily:
                return self._review_count_by_day_sql(where_clause, days or 30)
            if is_branch:
                return self._review_count_by_branch_sql(where_clause, days if (days or has_recent_hint) else None)
            if day_offset is not None:
                return self._review_count_sql(where_clause, days=1, day_offset=day_offset)
            if days is not None:
                return self._review_count_sql(where_clause, days=days)
            if has_recent_hint:
                return self._review_count_sql(where_clause, days=7)
            return self._review_count_sql(where_clause)

        return ""

    @staticmethod
    def _with_base_reviews_cte(
        where_clause: str,
        days: Optional[int] = None,
        day_offset: Optional[int] = None,
    ) -> str:
        if day_offset is not None:
            offset = max(0, day_offset)
            return f"""
WITH scoped AS (
  SELECT * FROM reviews
  WHERE {where_clause}
),
latest AS (
  SELECT MAX(review_date) AS max_date FROM scoped
),
base_reviews AS (
  SELECT s.*
  FROM scoped s
  CROSS JOIN latest l
  WHERE s.review_date IS NOT NULL
    AND l.max_date IS NOT NULL
    AND s.review_date = l.max_date - INTERVAL {offset} DAY
)
""".strip()

        if days is not None:
            safe_days = max(1, min(days, 365))
            lookback_days = safe_days - 1
            return f"""
WITH scoped AS (
  SELECT * FROM reviews
  WHERE {where_clause}
),
latest AS (
  SELECT MAX(review_date) AS max_date FROM scoped
),
base_reviews AS (
  SELECT s.*
  FROM scoped s
  CROSS JOIN latest l
  WHERE s.review_date IS NOT NULL
    AND l.max_date IS NOT NULL
    AND s.review_date >= l.max_date - INTERVAL {lookback_days} DAY
    AND s.review_date <= l.max_date
)
""".strip()

        return f"""
WITH base_reviews AS (
  SELECT * FROM reviews
  WHERE {where_clause}
)
""".strip()

    def _review_count_sql(
        self,
        where_clause: str,
        days: Optional[int] = None,
        day_offset: Optional[int] = None,
    ) -> str:
        with_clause = self._with_base_reviews_cte(
            where_clause=where_clause,
            days=days,
            day_offset=day_offset,
        )
        return f"""
{with_clause}
SELECT
  COUNT(*) AS review_count,
  MIN(review_date) AS start_date,
  MAX(review_date) AS end_date
FROM base_reviews
""".strip()

    def _review_count_by_branch_sql(self, where_clause: str, days: Optional[int] = None) -> str:
        with_clause = self._with_base_reviews_cte(where_clause=where_clause, days=days)
        return f"""
{with_clause}
SELECT
  branch_name,
  COUNT(*) AS review_count
FROM base_reviews
GROUP BY branch_name
ORDER BY review_count DESC
LIMIT 20
""".strip()

    def _review_count_by_day_sql(self, where_clause: str, days: int) -> str:
        with_clause = self._with_base_reviews_cte(where_clause=where_clause, days=days)
        return f"""
{with_clause}
SELECT
  review_date,
  COUNT(*) AS review_count
FROM base_reviews
GROUP BY review_date
ORDER BY review_date ASC
""".strip()

    def _waiting_metric_sql(
        self,
        where_clause: str,
        days: Optional[int] = None,
        day_offset: Optional[int] = None,
    ) -> str:
        with_clause = self._with_base_reviews_cte(
            where_clause=where_clause,
            days=days,
            day_offset=day_offset,
        )
        return f"""
{with_clause},
waiting AS (
  SELECT * FROM base_reviews
  WHERE regexp_matches(review_content, '{WAITING_SQL_REGEX}', 'i')
)
SELECT
  COUNT(*) AS waiting_review_count,
  ROUND(100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM base_reviews), 0), 1) AS waiting_ratio_pct,
  MIN(review_date) AS start_date,
  MAX(review_date) AS end_date
FROM waiting
""".strip()

    def _waiting_by_branch_sql(self, where_clause: str, days: Optional[int] = None) -> str:
        with_clause = self._with_base_reviews_cte(where_clause=where_clause, days=days)
        return f"""
{with_clause},
waiting AS (
  SELECT * FROM base_reviews
  WHERE regexp_matches(review_content, '{WAITING_SQL_REGEX}', 'i')
)
SELECT
  branch_name,
  COUNT(*) AS waiting_review_count
FROM waiting
GROUP BY branch_name
ORDER BY waiting_review_count DESC
LIMIT 20
""".strip()

    def _waiting_by_day_sql(self, where_clause: str, days: int) -> str:
        with_clause = self._with_base_reviews_cte(where_clause=where_clause, days=days)
        return f"""
{with_clause},
waiting AS (
  SELECT * FROM base_reviews
  WHERE regexp_matches(review_content, '{WAITING_SQL_REGEX}', 'i')
)
SELECT
  review_date,
  COUNT(*) AS waiting_review_count
FROM waiting
GROUP BY review_date
ORDER BY review_date ASC
""".strip()

    def _negative_metric_sql(
        self,
        where_clause: str,
        days: Optional[int] = None,
        day_offset: Optional[int] = None,
    ) -> str:
        with_clause = self._with_base_reviews_cte(
            where_clause=where_clause,
            days=days,
            day_offset=day_offset,
        )
        return f"""
{with_clause},
negative_reviews AS (
  SELECT * FROM base_reviews
  WHERE regexp_matches(review_content, '{NEGATIVE_ANY_SQL_REGEX}', 'i')
)
SELECT
  COUNT(*) AS negative_review_count,
  ROUND(100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM base_reviews), 0), 1) AS negative_ratio_pct,
  MIN(review_date) AS start_date,
  MAX(review_date) AS end_date
FROM negative_reviews
""".strip()

    def _negative_by_branch_sql(self, where_clause: str, days: Optional[int] = None) -> str:
        with_clause = self._with_base_reviews_cte(where_clause=where_clause, days=days)
        return f"""
{with_clause},
negative_reviews AS (
  SELECT * FROM base_reviews
  WHERE regexp_matches(review_content, '{NEGATIVE_ANY_SQL_REGEX}', 'i')
)
SELECT
  branch_name,
  COUNT(*) AS negative_review_count
FROM negative_reviews
GROUP BY branch_name
ORDER BY negative_review_count DESC
LIMIT 20
""".strip()

    def _negative_by_day_sql(self, where_clause: str, days: int) -> str:
        with_clause = self._with_base_reviews_cte(where_clause=where_clause, days=days)
        return f"""
{with_clause},
negative_reviews AS (
  SELECT * FROM base_reviews
  WHERE regexp_matches(review_content, '{NEGATIVE_ANY_SQL_REGEX}', 'i')
)
SELECT
  review_date,
  COUNT(*) AS negative_review_count
FROM negative_reviews
GROUP BY review_date
ORDER BY review_date ASC
""".strip()

    @staticmethod
    def _latest_reviews_sql(where_clause: str) -> str:
        return f"""
SELECT
  review_id,
  review_date,
  branch_name,
  nickname,
  review_content
FROM reviews
WHERE {where_clause}
ORDER BY review_date DESC
LIMIT 20
""".strip()

    @staticmethod
    def _is_negative_signal_intent(question: str) -> bool:
        lowered = question.lower()
        repeat_tokens = ("반복", "자주", "많이", "빈번", "계속")
        negative_tokens = ("부정", "불만", "아쉬", "문제", "불편", "개선")
        signal_tokens = ("신호", "패턴", "이슈", "포인트")

        repeat_and_negative = any(token in lowered for token in repeat_tokens) and any(
            token in lowered for token in negative_tokens
        )
        negative_and_signal = any(token in lowered for token in negative_tokens) and any(
            token in lowered for token in signal_tokens
        )
        return repeat_and_negative or negative_and_signal

    @staticmethod
    def _negative_signal_sql(where_clause: str) -> str:
        return f"""
WITH scoped AS (
  SELECT * FROM reviews
  WHERE {where_clause}
),
signals(signal, pattern) AS (
  VALUES
    ('웨이팅/대기', '웨이팅|대기|기다리'),
    ('혼잡/소음', '시끄럽|복잡|혼잡|사람\\\\s*많'),
    ('서비스 속도', '늦|느리|오래\\\\s*걸'),
    ('서비스 태도', '불친절|응대\\\\s*별로|서비스\\\\s*별로'),
    ('공간/좌석', '좁|자리\\\\s*없|좌석'),
    ('가격/가성비 불만', '비싸|가격\\\\s*부담|가성비\\\\s*별로'),
    ('맛 디테일 불만', '짜|싱겁|아쉽|별로|물리')
),
counts AS (
  SELECT
    signal,
    COUNT(*) FILTER (WHERE regexp_matches(review_content, pattern, 'i')) AS mention_count
  FROM scoped
  CROSS JOIN signals
  GROUP BY signal
)
SELECT
  signal,
  mention_count,
  ROUND(100.0 * mention_count / NULLIF((SELECT COUNT(*) FROM scoped), 0), 1) AS ratio_pct
FROM counts
ORDER BY mention_count DESC
LIMIT 10
""".strip()

    def _fallback_sql(self, question: str) -> str:
        where_clause = self._branch_where_clause(question)
        metric_sql = self._metric_template_sql(question, where_clause)
        if metric_sql:
            return metric_sql

        template_sql = self._fast_template_sql(question)
        if template_sql:
            return template_sql
        return self._latest_reviews_sql(where_clause)

    @staticmethod
    def _branch_where_clause(question: str) -> str:
        branch_filters = {
            "강남": "branch_name = '강남점'",
            "건대": "branch_name = '건대점'",
            "종각": "branch_name = '종각점'",
            "한양대": "branch_name = '한양대점'",
        }
        for token, clause in branch_filters.items():
            if token in question:
                return clause
        return "1=1"

    def _generate_sql(self, question: str) -> str:
        user_prompt = f"사용자 질문: {question}"
        response = self.client.chat.completions.create(
            model=self.model,
            temperature=self.openai_temperature,
            messages=[
                {
                    "role": "system",
                    "content": (
                        SQL_SYSTEM_PROMPT
                        + "\nReturn JSON when possible. If JSON fails, return SQL only."
                    ),
                },
                {"role": "user", "content": user_prompt},
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
            for block in re.findall(r"```(?:sql)?\\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL):
                candidate = block.strip()
                if candidate.lower().startswith(("select", "with")):
                    return candidate

        lowered = text.lower()
        select_pos = lowered.find("select")
        with_pos = lowered.find("with")

        starts = [pos for pos in [select_pos, with_pos] if pos != -1]
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
        for pattern in UNSUPPORTED_FUNCTION_PATTERNS:
            if pattern.search(sql):
                raise ValueError("Unsupported SQL function detected for DuckDB.")
        if "reviews" not in lowered:
            raise ValueError("SQL must reference reviews table.")

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

    def _select_scope(self, question: str) -> Tuple[pd.DataFrame, str]:
        full = self.data_store.get_full_reviews()
        mapping = {
            "강남": "강남점",
            "건대": "건대점",
            "종각": "종각점",
            "한양대": "한양대점",
        }

        for token, branch in mapping.items():
            if token in question:
                scoped = full[full["branch_name"].str.contains(branch, na=False)]
                if len(scoped) > 0:
                    return scoped, branch
        return full, "전체 지점"

    def _compute_negative_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        if len(df) == 0:
            return pd.DataFrame(columns=["신호", "언급수", "비율", "반복성"])

        rows: List[Dict[str, str]] = []
        total = len(df)
        text = df["review_content"].fillna("")

        for signal, pattern in NEGATIVE_SIGNAL_PATTERNS.items():
            count = int(text.str.contains(pattern, regex=True, case=False).sum())
            ratio = (count / total) * 100
            repetitive = "반복적" if (count >= 5 and ratio >= 3.0) else "산발적"
            rows.append(
                {
                    "신호": signal,
                    "언급수": count,
                    "비율": f"{ratio:.1f}%",
                    "반복성": repetitive,
                }
            )

        ranked = sorted(rows, key=lambda row: row["언급수"], reverse=True)
        return pd.DataFrame(ranked)

    def _find_hidden_negatives(self, df: pd.DataFrame) -> List[str]:
        if len(df) == 0:
            return []

        examples: List[Tuple[int, str]] = []
        for review in df["review_content"].fillna(""):
            has_positive = bool(POSITIVE_HINT.search(review))
            has_negative = bool(NEGATIVE_HINT.search(review))
            if not (has_positive and has_negative):
                continue

            score = 1
            if "근데" in review or "하지만" in review or "다만" in review:
                score += 1
            if "아쉽" in review or "별로" in review:
                score += 1

            compact = re.sub(r"\s+", " ", review).strip()
            if len(compact) > 160:
                compact = compact[:157] + "..."
            examples.append((score, compact))

        examples.sort(key=lambda item: item[0], reverse=True)
        return [text for _, text in examples[:5]]

    def _count_hidden_negative_reviews(self, df: pd.DataFrame) -> int:
        if len(df) == 0:
            return 0
        text = df["review_content"].fillna("")
        mask = text.str.contains(POSITIVE_HINT, regex=True) & text.str.contains(
            NEGATIVE_HINT, regex=True
        )
        return int(mask.sum())

    def _compute_revisit_metrics(self, df: pd.DataFrame) -> Dict[str, Optional[float]]:
        metrics: Dict[str, Optional[float]] = {
            "total_reviews": float(len(df)),
            "unique_customers": 0.0,
            "repeat_customers": 0.0,
            "repeat_rate": 0.0,
            "revisit_intent_reviews": 0.0,
            "revisit_intent_rate": 0.0,
            "avg_interval_days": None,
            "median_interval_days": None,
            "interval_samples": 0.0,
        }

        if len(df) == 0:
            return metrics

        text = df["review_content"].fillna("")
        revisit_intent_reviews = int(text.str.contains(REVISIT_INTENT_HINT, regex=True).sum())
        metrics["revisit_intent_reviews"] = float(revisit_intent_reviews)
        metrics["revisit_intent_rate"] = (revisit_intent_reviews / len(df)) * 100

        work = df.copy()
        work["nickname"] = work["nickname"].fillna("").astype(str).str.strip()
        work = work[(work["nickname"] != "") & work["review_date"].notna()].copy()
        if len(work) == 0:
            return metrics

        counts = work.groupby("nickname")["review_id"].count()
        unique_customers = int(counts.shape[0])
        repeat_customers = int((counts >= 2).sum())

        metrics["unique_customers"] = float(unique_customers)
        metrics["repeat_customers"] = float(repeat_customers)
        metrics["repeat_rate"] = (
            (repeat_customers / unique_customers) * 100 if unique_customers else 0.0
        )

        intervals: List[int] = []
        sorted_work = work.sort_values("review_date")
        for _, group in sorted_work.groupby("nickname"):
            dates = (
                pd.to_datetime(group["review_date"], errors="coerce")
                .dropna()
                .sort_values()
                .drop_duplicates()
            )
            if len(dates) < 2:
                continue
            diffs = dates.diff().dropna().dt.days
            intervals.extend(int(day) for day in diffs if day > 0)

        if intervals:
            interval_series = pd.Series(intervals, dtype="float64")
            metrics["avg_interval_days"] = float(interval_series.mean())
            metrics["median_interval_days"] = float(interval_series.median())
            metrics["interval_samples"] = float(len(intervals))

        return metrics

    @staticmethod
    def _negative_ratio(df: pd.DataFrame) -> float:
        if len(df) == 0:
            return 0.0
        text = df["review_content"].fillna("")
        mentions = int(text.str.contains(NEGATIVE_ANY_PATTERN, regex=True).sum())
        return (mentions / len(df)) * 100

    def _compute_branch_negative_density(self, df: pd.DataFrame) -> pd.DataFrame:
        if len(df) == 0:
            return pd.DataFrame(columns=["지점명", "리뷰수", "부정리뷰수", "부정비율"])

        work = df.copy()
        text = work["review_content"].fillna("")
        work["is_negative"] = text.str.contains(NEGATIVE_ANY_PATTERN, regex=True)

        grouped = (
            work.groupby("branch_name")
            .agg(리뷰수=("review_id", "count"), 부정리뷰수=("is_negative", "sum"))
            .reset_index()
            .rename(columns={"branch_name": "지점명"})
        )
        grouped["부정비율"] = (
            100.0 * grouped["부정리뷰수"] / grouped["리뷰수"].where(grouped["리뷰수"] != 0, 1)
        )
        grouped = grouped.sort_values("부정비율", ascending=False)
        grouped["부정비율"] = grouped["부정비율"].map(lambda value: f"{value:.1f}%")
        return grouped

    def _compute_recent_negative_delta(self, df: pd.DataFrame) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        work = df[df["review_date"].notna()].copy()
        if len(work) < 20:
            return None, None, None

        work["review_date"] = pd.to_datetime(work["review_date"], errors="coerce")
        work = work[work["review_date"].notna()].copy()
        if len(work) < 20:
            return None, None, None

        max_date = work["review_date"].max()
        recent_start = max_date - pd.Timedelta(days=29)
        prev_start = max_date - pd.Timedelta(days=59)
        prev_end = max_date - pd.Timedelta(days=30)

        recent_df = work[(work["review_date"] >= recent_start) & (work["review_date"] <= max_date)]
        prev_df = work[(work["review_date"] >= prev_start) & (work["review_date"] <= prev_end)]

        if len(recent_df) < 10 or len(prev_df) < 10:
            return None, None, None

        recent_ratio = self._negative_ratio(recent_df)
        prev_ratio = self._negative_ratio(prev_df)
        return recent_ratio, prev_ratio, recent_ratio - prev_ratio

    def _build_markdown(
        self,
        question: str,
        sql: str,
        query_result: pd.DataFrame,
        signal_df: pd.DataFrame,
        hidden_examples: List[str],
        scope_label: str,
        scope_df: pd.DataFrame,
    ) -> str:
        if self._should_use_compact_answer(question, query_result):
            return self._build_compact_markdown(
                question=question,
                sql=sql,
                query_result=query_result,
                scope_label=scope_label,
            )

        revisit_metrics = self._compute_revisit_metrics(scope_df)
        lines: List[str] = []
        lines.append("## 리뷰 분석 결과")
        lines.append(f"- 질문: {question}")
        lines.append(f"- 분석 범위: {scope_label}")
        lines.append("")

        lines.append("### 1) 핵심 결과")
        if len(query_result) == 0:
            lines.append("조회 결과가 없습니다.")
        else:
            preview = self._mask_sensitive_df(query_result.head(self.max_table_rows))
            preview = self._localize_columns(preview)
            lines.append(self._df_to_markdown(preview))
            if len(query_result) > self.max_table_rows:
                lines.append("")
                lines.append(
                    f"_표시는 상위 {self.max_table_rows}행입니다. (전체 {len(query_result)}행)_"
                )
        lines.append("")

        lines.append("### 2) 반복 부정 신호")
        if len(signal_df) == 0:
            lines.append("신호를 계산할 데이터가 없습니다.")
        else:
            lines.append(self._df_to_markdown(self._localize_columns(signal_df.head(7))))
        lines.append("")

        lines.append("### 3) 긍정 속 숨은 불만 예시")
        if not hidden_examples:
            lines.append("숨은 불만 패턴이 뚜렷하게 감지되지 않았습니다.")
        else:
            for idx, text in enumerate(hidden_examples, start=1):
                lines.append(f"{idx}. {self._mask_text_pii(text)}")
        lines.append("")

        lines.append("### 4) 재방문 지표 (닉네임 기준 추정)")
        unique_customers = int(revisit_metrics["unique_customers"] or 0)
        repeat_customers = int(revisit_metrics["repeat_customers"] or 0)
        repeat_rate = float(revisit_metrics["repeat_rate"] or 0.0)
        revisit_intent_reviews = int(revisit_metrics["revisit_intent_reviews"] or 0)
        revisit_intent_rate = float(revisit_metrics["revisit_intent_rate"] or 0.0)
        total_reviews = int(revisit_metrics["total_reviews"] or 0)
        lines.append(
            f"- 재방문 고객: **{repeat_customers}명 / {unique_customers}명** ({repeat_rate:.1f}%)"
        )
        lines.append(
            f"- 리뷰 내 재방문 의사 언급: **{revisit_intent_reviews}건 / {total_reviews}건** ({revisit_intent_rate:.1f}%)"
        )

        avg_interval_days = revisit_metrics["avg_interval_days"]
        median_interval_days = revisit_metrics["median_interval_days"]
        interval_samples = int(revisit_metrics["interval_samples"] or 0)
        if avg_interval_days is not None and median_interval_days is not None and interval_samples > 0:
            lines.append(
                f"- 방문 간격 추정: 평균 **{avg_interval_days:.1f}일**, 중앙값 **{median_interval_days:.1f}일** (표본 {interval_samples}건)"
            )
        else:
            lines.append("- 방문 간격 추정: 동일 닉네임의 재방문 데이터가 부족해 산출하지 못했습니다.")
        lines.append("")

        lines.append("### 5) 점주 인사이트")
        lines.extend(
            self._build_interpretation(
                signal_df=signal_df,
                hidden_examples=hidden_examples,
                scope_df=scope_df,
                revisit_metrics=revisit_metrics,
            )
        )
        lines.append("")

        lines.append("```sql")
        lines.append(sql)
        lines.append("```")

        return "\n".join(lines).strip()

    def _should_use_compact_answer(self, question: str, query_result: pd.DataFrame) -> bool:
        if self._is_structured_metric_intent(question):
            return True

        lowered = question.lower()
        direct_tokens = ("몇개", "몇 개", "개수", "몇건", "몇 건", "건수", "count", "얼마")
        has_direct_metric_question = any(token in lowered for token in direct_tokens)
        is_small_result = len(query_result) <= 3 and len(query_result.columns) <= 4
        return has_direct_metric_question and is_small_result

    def _is_structured_metric_intent(self, question: str) -> bool:
        return (
            self._is_review_metric_intent(question)
            or self._is_waiting_metric_intent(question)
            or self._is_negative_metric_intent(question)
        )

    def _build_compact_markdown(
        self,
        question: str,
        sql: str,
        query_result: pd.DataFrame,
        scope_label: str,
    ) -> str:
        lines: List[str] = []
        lines.append("## 리뷰 분석 결과")
        lines.append(f"- 질문: {question}")
        lines.append(f"- 분석 범위: {scope_label}")
        lines.append("")

        lines.append("### 답변")
        lines.append(self._summarize_compact_answer(question, query_result))
        lines.append("")

        lines.append("### 결과 표")
        if len(query_result) == 0:
            lines.append("조회 결과가 없습니다.")
        else:
            preview = self._mask_sensitive_df(query_result.head(self.max_table_rows))
            preview = self._localize_columns(preview)
            lines.append(self._df_to_markdown(preview))
            if len(query_result) > self.max_table_rows:
                lines.append("")
                lines.append(
                    f"_표시는 상위 {self.max_table_rows}행입니다. (전체 {len(query_result)}행)_"
                )
        lines.append("")

        lines.append("```sql")
        lines.append(sql)
        lines.append("```")
        return "\n".join(lines).strip()

    def _summarize_compact_answer(self, question: str, query_result: pd.DataFrame) -> str:
        if len(query_result) == 0:
            return "- 조회 결과가 없습니다."

        row = query_result.iloc[0]
        column_lookup = {str(column).strip().lower(): column for column in query_result.columns}

        branch_column = self._find_column_name(column_lookup, candidates=("branch_name", "지점명"))
        date_column = self._find_column_name(
            column_lookup,
            candidates=("review_date", "date", "review_day", "일자"),
        )

        waiting_count_column = self._find_column_name(
            column_lookup,
            candidates=("waiting_review_count", "waiting_count"),
        )
        negative_count_column = self._find_column_name(
            column_lookup,
            candidates=("negative_review_count",),
        )
        count_column = self._find_column_name(
            column_lookup,
            candidates=("review_count", "count", "cnt", "review_cnt", "건수", "개수", "mention_count"),
        )
        metric_label = "리뷰"
        if waiting_count_column is not None:
            count_column = waiting_count_column
            metric_label = "웨이팅 언급 리뷰"
        elif negative_count_column is not None:
            count_column = negative_count_column
            metric_label = "부정 신호 리뷰"

        ratio_column = self._find_column_name(
            column_lookup,
            candidates=("waiting_ratio_pct", "negative_ratio_pct", "ratio_pct", "ratio", "pct_of_recent"),
        )

        if count_column is None:
            for column in query_result.columns:
                try:
                    numeric_value = pd.to_numeric(row[column], errors="coerce")
                except Exception:
                    numeric_value = None
                if numeric_value is not None and not pd.isna(numeric_value):
                    count_column = column
                    break

        if count_column is None:
            return "- 요청하신 결과를 표로 정리했습니다."

        count_value = pd.to_numeric(row[count_column], errors="coerce")
        if pd.isna(count_value):
            return "- 요청하신 결과를 표로 정리했습니다."

        numeric_series = pd.to_numeric(query_result[count_column], errors="coerce").fillna(0)
        if branch_column is not None and len(query_result) > 1:
            top_idx = numeric_series.idxmax()
            top_count = int(numeric_series.loc[top_idx])
            top_branch = str(query_result.loc[top_idx, branch_column])
            total_count = int(numeric_series.sum())
            return (
                f"- 지점별 {metric_label}를 집계했습니다. 총 **{total_count:,}건**, "
                f"가장 많은 지점은 **{top_branch} ({top_count:,}건)**입니다."
            )

        if date_column is not None and len(query_result) > 1:
            top_idx = numeric_series.idxmax()
            top_count = int(numeric_series.loc[top_idx])
            top_date = self._format_date(query_result.loc[top_idx, date_column])
            start_date = self._format_date(query_result[date_column].min())
            end_date = self._format_date(query_result[date_column].max())
            total_count = int(numeric_series.sum())
            period_text = f"{start_date} ~ {end_date}" if start_date and end_date else "집계 구간"
            return (
                f"- 일자별 {metric_label}를 집계했습니다. 총 **{total_count:,}건**이며, "
                f"가장 많은 날은 **{top_date} ({top_count:,}건)**입니다. (기간: {period_text})"
            )

        count_text = f"{int(count_value):,}"
        days = self._extract_recent_days(question)
        day_offset = self._extract_single_day_offset(question)
        start_column = self._find_column_name(
            column_lookup,
            candidates=("start_date", "min_date", "from_date", "period_start", "시작일"),
        )
        end_column = self._find_column_name(
            column_lookup,
            candidates=("end_date", "max_date", "to_date", "period_end", "종료일"),
        )

        period_text = ""
        if start_column and end_column:
            start_text = self._format_date(row[start_column])
            end_text = self._format_date(row[end_column])
            if start_text and end_text:
                period_text = f"집계 기간: {start_text} ~ {end_text}"

        ratio_text = ""
        if ratio_column is not None:
            ratio_value = pd.to_numeric(row[ratio_column], errors="coerce")
            if ratio_value is not None and not pd.isna(ratio_value):
                ratio_text = f"비율: {float(ratio_value):.1f}%"

        context_prefix = ""
        if day_offset == 0:
            context_prefix = "오늘 기준 "
        elif day_offset == 1:
            context_prefix = "어제 기준 "
        elif day_offset == 2:
            context_prefix = "그저께 기준 "
        elif days:
            context_prefix = f"최근 {days}일 기준 "
        elif self._has_recent_hint(question):
            context_prefix = "최근 기준 "

        extras = [text for text in (period_text, ratio_text) if text]
        extras_text = f" ({', '.join(extras)})" if extras else ""
        return f"- {context_prefix}{metric_label} 개수는 **{count_text}건**입니다.{extras_text}"

    @staticmethod
    def _find_column_name(
        column_lookup: Dict[str, object],
        candidates: Tuple[str, ...],
    ) -> Optional[object]:
        for candidate in candidates:
            found = column_lookup.get(candidate.lower())
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

    def _vary_sentence(self, variants: List[str], **kwargs: object) -> str:
        if not variants:
            return ""

        temperature = self.answer_variation_temperature
        if len(variants) == 1 or temperature <= 0.0:
            template = variants[0]
        elif temperature < 0.35:
            template = variants[self._rng.randrange(min(2, len(variants)))]
        else:
            template = variants[self._rng.randrange(len(variants))]

        return template.format(**kwargs)

    def _build_interpretation(
        self,
        signal_df: pd.DataFrame,
        hidden_examples: List[str],
        scope_df: pd.DataFrame,
        revisit_metrics: Dict[str, Optional[float]],
    ) -> List[str]:
        bullets: List[str] = []

        if len(signal_df) > 0:
            top = signal_df.iloc[0]
            bullets.append(
                self._vary_sentence(
                    [
                        "- 가장 강한 부정 신호는 **{signal}**이며, {count}건({ratio})으로 관찰됩니다.",
                        "- 현재 기준 최상위 이슈는 **{signal}**으로, 총 {count}건({ratio}) 언급되었습니다.",
                        "- 부정 패턴 1순위는 **{signal}**입니다. 규모는 {count}건({ratio}) 수준입니다.",
                    ],
                    signal=top["신호"],
                    count=top["언급수"],
                    ratio=top["비율"],
                )
            )

            repetitive = signal_df[signal_df["반복성"] == "반복적"]
            if len(repetitive) > 0:
                names = ", ".join(repetitive["신호"].head(3).tolist())
                bullets.append(
                    self._vary_sentence(
                        [
                            "- 반복적 신호(구조적 이슈 가능성): **{names}**",
                            "- 일회성이 아닌 반복성 이슈는 **{names}**로 확인됩니다.",
                            "- 운영 프로세스 점검이 필요한 반복 신호는 **{names}**입니다.",
                        ],
                        names=names,
                    )
                )
            else:
                bullets.append(
                    self._vary_sentence(
                        [
                            "- 반복적 패턴보다 산발적 불만이 많아, 운영 이슈보다 특정 상황 이슈 가능성이 큽니다.",
                            "- 구조적 문제보다는 특정 시간대/상황에서 발생한 불만이 상대적으로 많습니다.",
                            "- 현재는 고정 이슈보다 단발성 이슈 성격이 더 강하게 나타납니다.",
                        ]
                    )
                )

        positive_reviews = int(
            scope_df["review_content"].fillna("").str.contains(POSITIVE_HINT, regex=True).sum()
        )
        hidden_count = self._count_hidden_negative_reviews(scope_df)
        if positive_reviews > 0:
            hidden_ratio = (hidden_count / positive_reviews) * 100
            bullets.append(
                self._vary_sentence(
                    [
                        "- 칭찬 표현이 포함된 리뷰 {positive_reviews}건 중 **{hidden_count}건({hidden_ratio:.1f}%)**에서 조건부 불만이 함께 나타났습니다.",
                        "- 긍정 리뷰 {positive_reviews}건 가운데 **{hidden_count}건({hidden_ratio:.1f}%)**은 칭찬과 불만이 동시에 존재합니다.",
                        "- 만족 코멘트가 있는 리뷰 중 **{hidden_count}건({hidden_ratio:.1f}%)**에서 숨은 불만 신호가 포착됐습니다.",
                    ],
                    positive_reviews=positive_reviews,
                    hidden_count=hidden_count,
                    hidden_ratio=hidden_ratio,
                )
            )

        branch_density = self._compute_branch_negative_density(scope_df)
        if len(branch_density) >= 2:
            worst = branch_density.iloc[0]
            best = branch_density.iloc[-1]
            bullets.append(
                self._vary_sentence(
                    [
                        "- 지점별 체감 품질 편차가 있습니다. 부정비율 최고는 **{worst_branch}({worst_ratio})**, 최저는 **{best_branch}({best_ratio})**입니다.",
                        "- 지점 간 불만 밀도 차이가 보입니다. 높은 쪽은 **{worst_branch}({worst_ratio})**, 낮은 쪽은 **{best_branch}({best_ratio})**입니다.",
                        "- 매장별 경험 편차가 확인됩니다. 부정비율 상위는 **{worst_branch}({worst_ratio})**, 하위는 **{best_branch}({best_ratio})**입니다.",
                    ],
                    worst_branch=worst["지점명"],
                    worst_ratio=worst["부정비율"],
                    best_branch=best["지점명"],
                    best_ratio=best["부정비율"],
                )
            )

        recent_ratio, prev_ratio, delta = self._compute_recent_negative_delta(scope_df)
        if recent_ratio is not None and prev_ratio is not None and delta is not None:
            direction = "상승" if delta > 0 else "하락"
            bullets.append(
                self._vary_sentence(
                    [
                        "- 최근 30일 부정 언급 비율은 **{recent_ratio:.1f}%**로, 직전 30일({prev_ratio:.1f}%) 대비 **{delta_abs:.1f}%p {direction}**했습니다.",
                        "- 부정 언급 추세는 최근 30일 **{recent_ratio:.1f}%**이며, 이전 30일 대비 **{delta_abs:.1f}%p {direction}**입니다.",
                        "- 최근 월간 부정비율은 **{recent_ratio:.1f}%**로 집계되었고, 전월 구간보다 **{delta_abs:.1f}%p {direction}**했습니다.",
                    ],
                    recent_ratio=recent_ratio,
                    prev_ratio=prev_ratio,
                    delta_abs=abs(delta),
                    direction=direction,
                )
            )

        repeat_rate = float(revisit_metrics["repeat_rate"] or 0.0)
        avg_interval_days = revisit_metrics["avg_interval_days"]
        if repeat_rate >= 25:
            if avg_interval_days is not None:
                bullets.append(
                    self._vary_sentence(
                        [
                            "- 재방문 비율이 **{repeat_rate:.1f}%**로 높은 편이며, 평균 방문 간격은 **{avg_interval_days:.1f}일**입니다. 멤버십/쿠폰 회전 주기를 이 간격에 맞추면 효율이 좋습니다.",
                            "- 재방문율 **{repeat_rate:.1f}%**와 평균 간격 **{avg_interval_days:.1f}일**을 기준으로 리텐션 쿠폰 주기를 설계하면 효과적입니다.",
                            "- 충성 고객 비중이 높습니다(재방문율 **{repeat_rate:.1f}%**). 평균 재방문 간격 **{avg_interval_days:.1f}일** 중심으로 CRM 타이밍을 맞추는 것을 권장합니다.",
                        ],
                        repeat_rate=repeat_rate,
                        avg_interval_days=avg_interval_days,
                    )
                )
            else:
                bullets.append(
                    self._vary_sentence(
                        [
                            "- 재방문 비율이 **{repeat_rate:.1f}%**로 높은 편입니다. 반복 방문 고객 전용 혜택 설계 여지가 큽니다.",
                            "- 재방문율이 **{repeat_rate:.1f}%**로 높아 단골 전용 혜택 실험 가치가 충분합니다.",
                            "- 반복 방문 고객 비중이 높은 편(**{repeat_rate:.1f}%**)이라 멤버십/리워드 전략 효과를 기대할 수 있습니다.",
                        ],
                        repeat_rate=repeat_rate,
                    )
                )
        elif repeat_rate > 0:
            bullets.append(
                self._vary_sentence(
                    [
                        "- 재방문 비율은 **{repeat_rate:.1f}%**입니다. 재방문 전환을 높이려면 첫 방문 직후 7일 내 리마인드 메시지가 효과적일 가능성이 큽니다.",
                        "- 현재 재방문율은 **{repeat_rate:.1f}%**입니다. 첫 방문 후 1주 이내 재접점 메시지로 전환을 끌어올릴 수 있습니다.",
                        "- 재방문율 **{repeat_rate:.1f}%** 구간에서는 방문 7일 이내 쿠폰/알림 발송이 재방문 유도에 유리합니다.",
                    ],
                    repeat_rate=repeat_rate,
                )
            )
        else:
            bullets.append(
                self._vary_sentence(
                    [
                        "- 닉네임 기준 재방문 고객이 충분히 잡히지 않았습니다. 리뷰 작성 유도 캠페인으로 고객 식별 가능한 표본을 먼저 늘리는 것이 좋습니다.",
                        "- 현재는 닉네임 기반 재방문 추정 표본이 부족합니다. 리뷰 참여 유도부터 강화하는 것이 우선입니다.",
                        "- 고객 식별 가능한 리뷰 표본이 적어 재방문 분석 신뢰도가 낮습니다. 리뷰 수집 모수를 먼저 키우는 전략이 필요합니다.",
                    ]
                )
            )

        if hidden_examples:
            bullets.append(
                self._vary_sentence(
                    [
                        "- 숨은 불만 문장을 별도로 모니터링하면 '평점은 높지만 재방문이 줄어드는 구간'을 조기에 포착할 수 있습니다.",
                        "- 칭찬 속 불만 문장을 따로 추적하면 겉으로 드러나지 않는 이탈 신호를 빠르게 잡을 수 있습니다.",
                        "- 숨은 불만 샘플을 주간 단위로 점검하면 평점 대비 매출 하락 구간을 조기 탐지하기 좋습니다.",
                    ]
                )
            )
        else:
            bullets.append(
                self._vary_sentence(
                    [
                        "- 현재 샘플에서는 긍정-부정 혼합 문장이 상대적으로 적습니다.",
                        "- 이번 범위에서는 칭찬과 불만이 함께 나타난 문장이 많지 않았습니다.",
                        "- 혼합 감정(칭찬+불만) 케이스 비중이 낮아 숨은 불만 신호는 약한 편입니다.",
                    ]
                )
            )

        return bullets

    @staticmethod
    def _localize_columns(df: pd.DataFrame) -> pd.DataFrame:
        if len(df.columns) == 0:
            return df
        renamed = {
            column: ReviewAnalysisService._to_korean_column(str(column)) for column in df.columns
        }
        return df.rename(columns=renamed)

    def _mask_sensitive_df(self, df: pd.DataFrame) -> pd.DataFrame:
        if len(df) == 0:
            return df

        masked = df.copy()
        for column in masked.columns:
            normalized = str(column).strip().lower()
            if self._is_identifier_column(normalized):
                masked[column] = masked[column].map(self._mask_identifier)
                continue
            if self._is_free_text_column(normalized):
                masked[column] = masked[column].map(self._mask_text_pii)
        return masked

    @staticmethod
    def _is_identifier_column(column_name: str) -> bool:
        tokens = (
            "nickname",
            "nick",
            "user_name",
            "username",
            "user_id",
            "customer_id",
            "member_id",
            "email",
            "phone",
            "tel",
            "contact",
            "연락처",
            "전화",
            "휴대폰",
            "핸드폰",
            "이메일",
            "닉네임",
            "아이디",
        )
        return any(token in column_name for token in tokens)

    @staticmethod
    def _is_free_text_column(column_name: str) -> bool:
        tokens = ("review_content", "content", "text", "message", "메모", "내용", "리뷰")
        return any(token in column_name for token in tokens)

    @staticmethod
    def _mask_identifier(value: object) -> str:
        if pd.isna(value):
            return ""
        text = str(value).strip()
        if text == "":
            return ""

        if "@" in text:
            return ReviewAnalysisService._mask_text_pii(text)

        if len(text) <= 1:
            return "*"
        if len(text) == 2:
            return text[0] + "*"
        return text[0] + ("*" * (len(text) - 2)) + text[-1]

    @staticmethod
    def _mask_text_pii(value: object) -> str:
        if pd.isna(value):
            return ""
        text = str(value)
        if text.strip() == "":
            return ""

        text = EMAIL_PATTERN.sub(
            lambda match: f"{match.group(1)}***@{match.group(2)}",
            text,
        )
        text = PHONE_PATTERN.sub(ReviewAnalysisService._mask_phone_number, text)
        text = HANDLE_PATTERN.sub(
            lambda match: f"@{match.group(1)[0]}***",
            text,
        )
        text = LONG_DIGIT_PATTERN.sub(ReviewAnalysisService._mask_long_digits, text)
        text = NAME_WITH_NIM_PATTERN.sub(
            lambda match: f"{ReviewAnalysisService._mask_name_like(match.group(1))}님",
            text,
        )
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def _mask_phone_number(match: re.Match) -> str:
        digits = re.sub(r"\D", "", match.group(0))
        if len(digits) < 9:
            return "***"
        prefix = digits[:3]
        suffix = digits[-4:]
        return f"{prefix}-****-{suffix}"

    @staticmethod
    def _mask_long_digits(match: re.Match) -> str:
        digits = match.group(0)
        if len(digits) <= 4:
            return "*" * len(digits)
        return digits[:2] + ("*" * (len(digits) - 4)) + digits[-2:]

    @staticmethod
    def _mask_name_like(name: str) -> str:
        if len(name) <= 1:
            return "*"
        if len(name) == 2:
            return name[0] + "*"
        return name[0] + ("*" * (len(name) - 2)) + name[-1]

    @staticmethod
    def _to_korean_column(column_name: str) -> str:
        normalized = column_name.strip().lower()
        mapping = {
            "review_id": "리뷰 번호",
            "review_count": "리뷰 개수",
            "signal": "신호",
            "mention_count": "언급 건수",
            "ratio_pct": "비율(%)",
            "review_date": "리뷰 일자",
            "start_date": "집계 시작일",
            "end_date": "집계 종료일",
            "branch_name": "지점명",
            "nickname": "닉네임",
            "review_content": "리뷰 내용",
            "wait_time": "대기시간",
            "waiting_count": "웨이팅 언급 건수",
            "waiting_review_count": "웨이팅 리뷰 건수",
            "waiting_ratio_pct": "웨이팅 비율(%)",
            "negative_review_count": "부정 리뷰 건수",
            "negative_ratio_pct": "부정 비율(%)",
            "keyword": "키워드",
            "occurrences": "언급 건수",
            "pct_of_recent": "최근 비율(%)",
            "count": "건수",
            "avg": "평균",
            "sum": "합계",
            "min": "최소",
            "max": "최대",
            "review_year": "리뷰 연도",
            "review_month": "리뷰 월",
            "review_day": "리뷰 일",
            "review_length": "리뷰 길이",
        }
        if normalized in mapping:
            return mapping[normalized]

        token_map = {
            "review": "리뷰",
            "branch": "지점",
            "name": "명",
            "date": "일자",
            "content": "내용",
            "nickname": "닉네임",
            "count": "건수",
            "ratio": "비율",
            "pct": "비율",
            "month": "월",
            "year": "연도",
            "day": "일",
            "length": "길이",
            "revisit": "재방문",
            "customer": "고객",
            "customers": "고객수",
            "interval": "간격",
            "avg": "평균",
            "median": "중앙값",
            "id": "번호",
        }
        translated_tokens = [token_map.get(token, token) for token in normalized.split("_")]
        translated = " ".join(translated_tokens).strip()
        return translated if translated != normalized else column_name

    @staticmethod
    def _df_to_markdown(df: pd.DataFrame) -> str:
        if len(df.columns) == 0:
            return "(empty table)"

        headers = [str(col) for col in df.columns]
        rows = [headers]
        rows += [
            [ReviewAnalysisService._clip_cell(v) for v in row] for row in df.values.tolist()
        ]

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
