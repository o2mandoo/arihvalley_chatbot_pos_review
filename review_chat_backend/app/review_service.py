import json
import re
from dataclasses import dataclass
from typing import Dict, List, Tuple

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

FORBIDDEN_SQL = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|replace|merge|grant|revoke)\b",
    re.IGNORECASE,
)


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
        sql = self._generate_sql_with_fast_path(question)
        result_df = self.data_store.query(sql)

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
        )
        return ReviewAnswer(
            sql=sql,
            markdown=markdown,
            row_count=len(result_df),
            scope=scope_label,
        )

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

        if ("반복" in question and "부정" in question) or (
            "부정" in question and "신호" in question
        ):
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

    def _build_markdown(
        self,
        question: str,
        sql: str,
        query_result: pd.DataFrame,
        signal_df: pd.DataFrame,
        hidden_examples: List[str],
        scope_label: str,
    ) -> str:
        lines: List[str] = []
        lines.append("## 리뷰 분석 결과")
        lines.append(f"- 질문: {question}")
        lines.append(f"- 분석 범위: {scope_label}")
        lines.append("")

        lines.append("### 1) 실행 SQL")
        lines.append("```sql")
        lines.append(sql)
        lines.append("```")
        lines.append("")

        lines.append("### 2) 쿼리 결과")
        if len(query_result) == 0:
            lines.append("조회 결과가 없습니다.")
        else:
            preview = query_result.head(self.max_table_rows)
            lines.append(self._df_to_markdown(preview))
            if len(query_result) > self.max_table_rows:
                lines.append("")
                lines.append(
                    f"_표시는 상위 {self.max_table_rows}행입니다. (전체 {len(query_result)}행)_"
                )
        lines.append("")

        lines.append("### 3) 반복 부정 신호")
        if len(signal_df) == 0:
            lines.append("신호를 계산할 데이터가 없습니다.")
        else:
            lines.append(self._df_to_markdown(signal_df.head(7)))
        lines.append("")

        lines.append("### 4) 긍정 속 숨은 불만 예시")
        if not hidden_examples:
            lines.append("숨은 불만 패턴이 뚜렷하게 감지되지 않았습니다.")
        else:
            for idx, text in enumerate(hidden_examples, start=1):
                lines.append(f"{idx}. {text}")
        lines.append("")

        lines.append("### 5) 해석")
        lines.extend(self._build_interpretation(signal_df, hidden_examples))

        return "\n".join(lines).strip()

    @staticmethod
    def _build_interpretation(signal_df: pd.DataFrame, hidden_examples: List[str]) -> List[str]:
        bullets: List[str] = []
        if len(signal_df) > 0:
            top = signal_df.iloc[0]
            bullets.append(
                f"- 가장 강한 부정 신호는 **{top['신호']}**이며, {top['언급수']}건({top['비율']})으로 관찰됩니다."
            )

            repetitive = signal_df[signal_df["반복성"] == "반복적"]
            if len(repetitive) > 0:
                names = ", ".join(repetitive["신호"].head(3).tolist())
                bullets.append(
                    f"- 반복적 신호(구조적 이슈 가능성): **{names}**"
                )
            else:
                bullets.append(
                    "- 반복적 패턴보다 산발적 불만이 많아, 운영 이슈보다 특정 상황 이슈 가능성이 큽니다."
                )

        if hidden_examples:
            bullets.append(
                "- 표면상 긍정 리뷰 내부에서도 조건부 불만이 함께 나타납니다. 개선 우선순위 산정 시 이 구간을 별도로 추적하세요."
            )
        else:
            bullets.append("- 현재 샘플에서는 긍정-부정 혼합 문장이 상대적으로 적습니다.")

        return bullets

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
