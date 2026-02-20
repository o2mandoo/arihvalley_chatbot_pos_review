import html
import json
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
NEGATIVE_HIGHLIGHT_PATTERN = re.compile(
    r"웨이팅|대기|기다리|시끄럽|복잡|혼잡|늦|느리|오래\s*걸|불친절|별로|아쉽|좁|자리\s*없|비싸|가성비\s*별로|짜|싱겁|물리",
    re.IGNORECASE,
)
NEGATIVE_ANY_PATTERN = re.compile(
    "|".join(f"(?:{pattern})" for pattern in NEGATIVE_SIGNAL_PATTERNS.values()),
    re.IGNORECASE,
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
        max_sql_rows: int = 200,
        max_table_rows: int = 20,
    ):
        self.data_store = data_store
        self.client = OpenAI(api_key=openai_api_key)
        self.model = openai_model
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
                safe_sql = self._negative_signal_sql(where_clause)
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
        template_sql = self._fast_template_sql(question)
        if template_sql:
            return template_sql
        return self._negative_signal_sql(where_clause)

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
            highlighted = self._highlight_negative_terms(compact)
            examples.append((score, highlighted))

        examples.sort(key=lambda item: item[0], reverse=True)
        return [text for _, text in examples[:5]]

    @staticmethod
    def _highlight_negative_terms(text: str) -> str:
        escaped = html.escape(text)
        return NEGATIVE_HIGHLIGHT_PATTERN.sub(
            lambda match: f'<span class="neg-highlight"><strong>{match.group(0)}</strong></span>',
            escaped,
        )

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
            preview = self._localize_columns(query_result.head(self.max_table_rows))
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
                lines.append(f"{idx}. {text}")
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

        lines.append("### 6) 분석 근거 (필요 시 확인)")
        lines.append("```sql")
        lines.append(sql)
        lines.append("```")

        return "\n".join(lines).strip()

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
                f"- 가장 강한 부정 신호는 **{top['신호']}**이며, {top['언급수']}건({top['비율']})으로 관찰됩니다."
            )

            repetitive = signal_df[signal_df["반복성"] == "반복적"]
            if len(repetitive) > 0:
                names = ", ".join(repetitive["신호"].head(3).tolist())
                bullets.append(f"- 반복적 신호(구조적 이슈 가능성): **{names}**")
            else:
                bullets.append(
                    "- 반복적 패턴보다 산발적 불만이 많아, 운영 이슈보다 특정 상황 이슈 가능성이 큽니다."
                )

        positive_reviews = int(
            scope_df["review_content"].fillna("").str.contains(POSITIVE_HINT, regex=True).sum()
        )
        hidden_count = self._count_hidden_negative_reviews(scope_df)
        if positive_reviews > 0:
            hidden_ratio = (hidden_count / positive_reviews) * 100
            bullets.append(
                f"- 칭찬 표현이 포함된 리뷰 {positive_reviews}건 중 **{hidden_count}건({hidden_ratio:.1f}%)**에서 조건부 불만이 함께 나타났습니다."
            )

        branch_density = self._compute_branch_negative_density(scope_df)
        if len(branch_density) >= 2:
            worst = branch_density.iloc[0]
            best = branch_density.iloc[-1]
            bullets.append(
                f"- 지점별 체감 품질 편차가 있습니다. 부정비율 최고는 **{worst['지점명']}({worst['부정비율']})**, 최저는 **{best['지점명']}({best['부정비율']})**입니다."
            )

        recent_ratio, prev_ratio, delta = self._compute_recent_negative_delta(scope_df)
        if recent_ratio is not None and prev_ratio is not None and delta is not None:
            direction = "상승" if delta > 0 else "하락"
            bullets.append(
                f"- 최근 30일 부정 언급 비율은 **{recent_ratio:.1f}%**로, 직전 30일({prev_ratio:.1f}%) 대비 **{abs(delta):.1f}%p {direction}**했습니다."
            )

        repeat_rate = float(revisit_metrics["repeat_rate"] or 0.0)
        avg_interval_days = revisit_metrics["avg_interval_days"]
        if repeat_rate >= 25:
            if avg_interval_days is not None:
                bullets.append(
                    f"- 재방문 비율이 **{repeat_rate:.1f}%**로 높은 편이며, 평균 방문 간격은 **{avg_interval_days:.1f}일**입니다. 멤버십/쿠폰 회전 주기를 이 간격에 맞추면 효율이 좋습니다."
                )
            else:
                bullets.append(
                    f"- 재방문 비율이 **{repeat_rate:.1f}%**로 높은 편입니다. 반복 방문 고객 전용 혜택 설계 여지가 큽니다."
                )
        elif repeat_rate > 0:
            bullets.append(
                f"- 재방문 비율은 **{repeat_rate:.1f}%**입니다. 재방문 전환을 높이려면 첫 방문 직후 7일 내 리마인드 메시지가 효과적일 가능성이 큽니다."
            )
        else:
            bullets.append(
                "- 닉네임 기준 재방문 고객이 충분히 잡히지 않았습니다. 리뷰 작성 유도 캠페인으로 고객 식별 가능한 표본을 먼저 늘리는 것이 좋습니다."
            )

        if hidden_examples:
            bullets.append(
                "- 숨은 불만 문장을 별도로 모니터링하면 '평점은 높지만 재방문이 줄어드는 구간'을 조기에 포착할 수 있습니다."
            )
        else:
            bullets.append("- 현재 샘플에서는 긍정-부정 혼합 문장이 상대적으로 적습니다.")

        return bullets

    @staticmethod
    def _localize_columns(df: pd.DataFrame) -> pd.DataFrame:
        if len(df.columns) == 0:
            return df
        renamed = {
            column: ReviewAnalysisService._to_korean_column(str(column)) for column in df.columns
        }
        return df.rename(columns=renamed)

    @staticmethod
    def _to_korean_column(column_name: str) -> str:
        normalized = column_name.strip().lower()
        mapping = {
            "review_id": "리뷰 번호",
            "signal": "신호",
            "mention_count": "언급 건수",
            "ratio_pct": "비율(%)",
            "review_date": "리뷰 일자",
            "branch_name": "지점명",
            "nickname": "닉네임",
            "review_content": "리뷰 내용",
            "wait_time": "대기시간",
            "waiting_count": "웨이팅 언급 건수",
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
