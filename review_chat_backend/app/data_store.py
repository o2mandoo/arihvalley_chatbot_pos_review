import re
from datetime import date
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

DATE_PATTERN = re.compile(r"(?P<y>\d{4})년\s*(?P<m>\d{1,2})월\s*(?P<d>\d{1,2})일")


class ReviewDataStore:
    def __init__(self, csv_path: Path):
        self.csv_path = csv_path
        self.conn = duckdb.connect(database=":memory:")
        self.reviews_df = self._load_reviews()
        self._register_tables()

    @staticmethod
    def _parse_korean_date(text: object) -> Optional[date]:
        if text is None:
            return None
        raw = str(text).strip()
        if not raw:
            return None
        match = DATE_PATTERN.search(raw)
        if not match:
            return None

        return date(
            int(match.group("y")),
            int(match.group("m")),
            int(match.group("d")),
        )

    def _load_reviews(self) -> pd.DataFrame:
        df = pd.read_csv(self.csv_path, encoding="utf-8-sig")

        normalized = pd.DataFrame(
            {
                "review_id": range(1, len(df) + 1),
                "branch_name": df["지점명"].fillna("").astype(str),
                "date_text": df["date"].fillna("").astype(str),
                "nickname": df["nickname"].fillna("").astype(str),
                "review_content": df["review_content"].fillna("").astype(str),
                "wait_time": df["wait_time"].fillna("").astype(str),
            }
        )

        normalized["review_date"] = pd.to_datetime(
            normalized["date_text"].apply(self._parse_korean_date), errors="coerce"
        )
        normalized["review_year"] = normalized["review_date"].dt.year
        normalized["review_month"] = normalized["review_date"].dt.month
        normalized["review_day"] = normalized["review_date"].dt.day
        normalized["review_length"] = normalized["review_content"].str.len()

        return normalized

    def _register_tables(self) -> None:
        self.conn.register("reviews_df", self.reviews_df)
        self.conn.execute(
            """
            CREATE OR REPLACE TABLE reviews AS
            SELECT
                review_id,
                branch_name,
                date_text,
                CAST(review_date AS DATE) AS review_date,
                review_year,
                review_month,
                review_day,
                nickname,
                review_content,
                wait_time,
                review_length
            FROM reviews_df
            """
        )

    def query(self, sql: str) -> pd.DataFrame:
        return self.conn.execute(sql).df()

    def get_full_reviews(self) -> pd.DataFrame:
        return self.reviews_df.copy()
