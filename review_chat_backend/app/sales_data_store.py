import io
import re
import unicodedata
from pathlib import Path
from typing import Dict, Optional, Tuple, Union

import duckdb
import msoffcrypto
import pandas as pd
from msoffcrypto.exceptions import FileFormatError, InvalidKeyError

ZIP_HEADER = b"PK\x03\x04"
OLE_HEADER = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"


PRIMARY_COLUMN_MAP: Dict[str, str] = {
    "지점명": "branch_name",
    "매장명": "branch_name",
    "지점": "branch_name",
    "매장": "branch_name",
    "주문기준일자": "order_base_date",
    "주문일자": "order_base_date",
    "주문기준 날짜": "order_base_date",
    "주문기준일시": "order_base_date",
    "주문번호": "order_number",
    "주문시작시각": "order_start_time",
    "주문시작시각(시분초)": "order_start_time",
    "주문시작시간": "order_start_time",
    "주문채널": "order_channel",
    "결제상태": "payment_status",
    "카테고리": "category",
    "상품명": "product_name",
    "수량": "quantity",
    "상품가격": "product_price",
    "옵션": "option_name",
    "옵션명": "option_name",
    "옵션가격": "option_price",
    "상품할인금액": "product_discount",
    "상품할인 금액": "product_discount",
    "주문할인금액": "order_discount",
    "주문할인 금액": "order_discount",
    "실판매금액": "actual_sales_amount",
    "실판매금액(할인,옵션포함)": "actual_sales_amount",
    "실판매금액(할인옵션포함)": "actual_sales_amount",
    "실판매금액(할인, 옵션 포함)": "actual_sales_amount",
    "과세여부": "tax_type",
    "부가세액": "vat_amount",
}

REQUIRED_COLUMNS: Dict[str, object] = {
    "branch_name": "",
    "order_base_date": pd.NaT,
    "order_number": "",
    "order_start_time": pd.NaT,
    "order_channel": "",
    "payment_status": "",
    "category": "",
    "product_name": "",
    "quantity": 0.0,
    "product_price": 0.0,
    "option_name": "",
    "option_price": 0.0,
    "product_discount": 0.0,
    "order_discount": 0.0,
    "actual_sales_amount": 0.0,
    "tax_type": "",
    "vat_amount": 0.0,
}

NUMERIC_COLUMNS = (
    "quantity",
    "product_price",
    "option_price",
    "product_discount",
    "order_discount",
    "actual_sales_amount",
    "vat_amount",
)

STRING_COLUMNS = (
    "branch_name",
    "order_number",
    "order_channel",
    "payment_status",
    "category",
    "product_name",
    "option_name",
    "tax_type",
)

DATE_COLUMNS = ("order_base_date", "order_start_time")


class SalesDataStore:
    def __init__(
        self,
        report_path: Path,
        excel_password: Optional[str] = None,
        default_branch_name: str = "왕십리한양대점",
    ):
        self.report_path = report_path
        self.excel_password = (excel_password or "").strip() or None
        self.default_branch_name = default_branch_name
        self.order_key_strategy = "order_number"
        self.order_key_stats: Dict[str, int] = {}
        self.conn = duckdb.connect(database=":memory:")
        self.selected_sheet_name = ""
        self.active_report_path = report_path
        self.sales_df = self._load_sales()
        self._register_tables()

    def set_source(self, report_path: Path, excel_password: Optional[str] = None) -> None:
        self.report_path = report_path
        if excel_password is not None:
            self.excel_password = (excel_password or "").strip() or None
        self.sales_df = self._load_sales()
        self._register_tables()

    def source_info(self) -> Dict[str, object]:
        return {
            "report_path": str(self.report_path),
            "active_report_path": str(self.active_report_path),
            "sheet_name": self.selected_sheet_name,
            "order_key_strategy": self.order_key_strategy,
            "order_key_stats": self.order_key_stats,
            "rows": len(self.sales_df),
            "columns": list(self.sales_df.columns),
            "encrypted": self._is_encrypted_file(),
            "fallback_used": str(self.active_report_path) != str(self.report_path),
        }

    def _is_encrypted_file(self) -> bool:
        try:
            with self.report_path.open("rb") as handle:
                office_file = msoffcrypto.OfficeFile(handle)
                return bool(office_file.is_encrypted())
        except Exception:
            return False

    def _find_decrypted_fallback(self) -> Optional[Path]:
        base = self.report_path
        if not base.exists():
            return None

        suffix = base.suffix
        stem = base.stem
        normalized_stem = unicodedata.normalize("NFC", stem)
        if normalized_stem.endswith("-decrypted"):
            return base

        candidate = base.with_name(f"{stem}-decrypted{suffix}")
        if candidate.exists() and self._looks_like_excel_file(candidate):
            return candidate
        return None

    @staticmethod
    def _looks_like_excel_file(path: Path) -> bool:
        try:
            with path.open("rb") as handle:
                header = handle.read(8)
            return header.startswith(ZIP_HEADER) or header.startswith(OLE_HEADER)
        except OSError:
            return False

    @staticmethod
    def _normalize_header(raw_header: object) -> str:
        text = str(raw_header or "").replace("\n", " ").strip()
        text = re.sub(r"\s+", " ", text)
        compact = re.sub(r"\s+", "", text)
        return compact

    @staticmethod
    def _to_safe_identifier(raw_header: str, index: int) -> str:
        normalized = unicodedata.normalize("NFKD", raw_header).encode("ascii", "ignore").decode("ascii")
        normalized = re.sub(r"[^0-9A-Za-z]+", "_", normalized).strip("_").lower()
        if not normalized:
            normalized = f"extra_col_{index + 1}"
        return normalized

    @staticmethod
    def _to_numeric(series: pd.Series) -> pd.Series:
        if pd.api.types.is_numeric_dtype(series):
            return pd.to_numeric(series, errors="coerce").fillna(0.0)
        cleaned = (
            series.fillna("")
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("원", "", regex=False)
            .str.replace(r"[^\d\.\-]", "", regex=True)
        )
        return pd.to_numeric(cleaned, errors="coerce").fillna(0.0)

    def _read_excel_source(self) -> Union[Path, io.BytesIO]:
        if not self.report_path.exists():
            raise FileNotFoundError(f"Sales report not found: {self.report_path}")

        decrypted_fallback = self._find_decrypted_fallback()
        try:
            with self.report_path.open("rb") as handle:
                office_file = msoffcrypto.OfficeFile(handle)
                if not office_file.is_encrypted():
                    self.active_report_path = self.report_path
                    return self.report_path

                if not self.excel_password:
                    if decrypted_fallback is not None and decrypted_fallback.exists():
                        self.active_report_path = decrypted_fallback
                        return decrypted_fallback
                    raise RuntimeError(
                        "매출 엑셀 파일이 암호화되어 있습니다. EXCEL_PASSWORD 환경변수를 설정해 주세요."
                    )

                try:
                    office_file.load_key(password=self.excel_password)
                except InvalidKeyError as exc:
                    raise RuntimeError(
                        "EXCEL_PASSWORD가 올바르지 않아 매출 엑셀 복호화에 실패했습니다."
                    ) from exc

                decrypted_buffer = io.BytesIO()
                try:
                    office_file.decrypt(decrypted_buffer)
                except Exception as exc:
                    raise RuntimeError(
                        "EXCEL_PASSWORD가 올바르지 않아 매출 엑셀 복호화에 실패했습니다."
                    ) from exc
                decrypted_buffer.seek(0)
                self.active_report_path = self.report_path
                return decrypted_buffer
        except FileFormatError:
            self.active_report_path = self.report_path
            return self.report_path

    @staticmethod
    def _read_excel_with_fallback_engines(excel_source: Union[Path, io.BytesIO]) -> Dict[str, pd.DataFrame]:
        errors: list[str] = []
        for engine in ("openpyxl", "xlrd"):
            try:
                if isinstance(excel_source, io.BytesIO):
                    excel_source.seek(0)
                return pd.read_excel(
                    excel_source,
                    sheet_name=None,
                    header=0,
                    engine=engine,
                )
            except Exception as exc:
                errors.append(f"{engine}: {exc}")

        raise RuntimeError("; ".join(errors) if errors else "엑셀 시트 파싱 실패")

    def _select_sheet(self, sheet_map: Dict[str, pd.DataFrame]) -> Tuple[str, pd.DataFrame]:
        if not sheet_map:
            raise RuntimeError("엑셀 시트가 비어 있습니다.")

        preferred_names = ("상품 주문 상세내역", "주문", "매출")
        scored: list[Tuple[int, int, int, str]] = []
        for name, frame in sheet_map.items():
            if frame is None:
                continue
            non_empty_rows = int(frame.dropna(how="all").shape[0])
            non_empty_cols = int(frame.dropna(axis=1, how="all").shape[1]) if frame.shape[1] > 0 else 0

            normalized_headers = [self._normalize_header(col) for col in frame.columns]
            mapped_hits = sum(1 for col in normalized_headers if col in PRIMARY_COLUMN_MAP)
            preferred_hit = int(any(token in str(name) for token in preferred_names))
            scored.append((preferred_hit, mapped_hits, non_empty_rows * max(non_empty_cols, 1), name))

        if not scored:
            first_name = next(iter(sheet_map.keys()))
            return first_name, sheet_map[first_name]

        scored.sort(reverse=True)
        selected_name = scored[0][3]
        return selected_name, sheet_map[selected_name]

    def _normalize_dataframe(self, frame: pd.DataFrame) -> pd.DataFrame:
        work = frame.copy()
        work = work.dropna(axis=0, how="all").dropna(axis=1, how="all")
        if work.empty:
            return pd.DataFrame(columns=list(REQUIRED_COLUMNS.keys()))

        renamed: Dict[str, str] = {}
        seen_names: set[str] = set()
        for idx, original in enumerate(work.columns):
            normalized = self._normalize_header(original)
            target = PRIMARY_COLUMN_MAP.get(normalized)
            if not target:
                target = self._to_safe_identifier(normalized, idx)
            unique_name = target
            suffix = 2
            while unique_name in seen_names:
                unique_name = f"{target}_{suffix}"
                suffix += 1
            seen_names.add(unique_name)
            renamed[original] = unique_name

        work = work.rename(columns=renamed)

        for column, default_value in REQUIRED_COLUMNS.items():
            if column not in work.columns:
                work[column] = default_value

        for column in DATE_COLUMNS:
            work[column] = pd.to_datetime(work[column], errors="coerce")

        for column in NUMERIC_COLUMNS:
            work[column] = self._to_numeric(work[column])

        work["quantity"] = (
            work["quantity"]
            .fillna(0)
            .round()
            .astype("Int64")
            .fillna(0)
            .astype(int)
        )
        work.loc[work["quantity"] <= 0, "quantity"] = 1

        for column in STRING_COLUMNS:
            work[column] = work[column].fillna("").astype(str).str.strip()

        if "branch_name" in work.columns:
            work.loc[work["branch_name"] == "", "branch_name"] = self.default_branch_name

        identity_mask = (work["order_number"] != "") | (work["product_name"] != "")
        amount_mask = work["actual_sales_amount"] != 0
        work = work[identity_mask | amount_mask].copy()

        order_base = pd.to_datetime(work["order_base_date"], errors="coerce")
        order_start = pd.to_datetime(work["order_start_time"], errors="coerce")
        work["sales_date"] = order_base.dt.date
        work["sales_year"] = order_base.dt.year
        work["sales_month"] = order_base.dt.month
        work["sales_day"] = order_base.dt.day
        work["sales_hour"] = order_start.dt.hour
        work["order_key"] = self._build_order_key(work=work, order_start=order_start)

        work["line_total_amount"] = (
            (work["product_price"] + work["option_price"]) * work["quantity"]
            - work["product_discount"]
            - work["order_discount"]
        )

        missing_sales_mask = work["actual_sales_amount"] == 0
        work.loc[missing_sales_mask, "actual_sales_amount"] = work.loc[
            missing_sales_mask, "line_total_amount"
        ]
        work["net_sales_amount"] = work["actual_sales_amount"]
        work["report_name"] = self.active_report_path.name
        work["sheet_name"] = self.selected_sheet_name
        work["sales_row_id"] = range(1, len(work) + 1)

        ordered_columns = [
            "sales_row_id",
            "report_name",
            "sheet_name",
            "branch_name",
            "order_base_date",
            "sales_date",
            "sales_year",
            "sales_month",
            "sales_day",
            "order_start_time",
            "sales_hour",
            "order_number",
            "order_key",
            "order_channel",
            "payment_status",
            "category",
            "product_name",
            "option_name",
            "tax_type",
            "quantity",
            "product_price",
            "option_price",
            "product_discount",
            "order_discount",
            "line_total_amount",
            "actual_sales_amount",
            "net_sales_amount",
            "vat_amount",
        ]

        extra_columns = [column for column in work.columns if column not in ordered_columns]
        return work[ordered_columns + sorted(extra_columns)]

    def _build_order_key(self, work: pd.DataFrame, order_start: pd.Series) -> pd.Series:
        order_number_key = (
            work["order_number"]
            .fillna("")
            .astype(str)
            .str.strip()
            .map(lambda value: f"no:{value}" if value else "")
        )
        start_time_key = (
            pd.to_datetime(order_start, errors="coerce")
            .dt.strftime("%Y-%m-%d %H:%M:%S")
            .fillna("")
            .map(lambda value: f"ts:{value}" if value else "")
        )

        non_empty_order_number = order_number_key[order_number_key != ""]
        non_empty_start_time = start_time_key[start_time_key != ""]
        distinct_order_number = int(non_empty_order_number.nunique()) if len(non_empty_order_number) > 0 else 0
        distinct_start_time = int(non_empty_start_time.nunique()) if len(non_empty_start_time) > 0 else 0
        # 일부 POS 리포트의 주문번호는 실제 주문 ID가 아니라 테이블 번호(예: 001, 테-003)라서 고유값이 매우 작다.
        # 이 경우 주문시작시각(초 단위) 기반 키를 우선 사용하면 주문 건수 과소집계/객단가 과대집계를 줄일 수 있다.
        use_start_time = bool(
            distinct_start_time > 0
            and (distinct_order_number == 0 or distinct_order_number <= max(100, int(distinct_start_time * 0.35)))
        )

        primary = start_time_key if use_start_time else order_number_key
        secondary = order_number_key if use_start_time else start_time_key
        fallback_row = pd.Series(
            [f"row:{idx}" for idx in range(1, len(work) + 1)],
            index=work.index,
            dtype="string",
        )
        order_key = primary.where(primary != "", secondary)
        order_key = order_key.where(order_key != "", fallback_row)
        order_key = order_key.fillna("").astype(str).str.strip()

        self.order_key_strategy = "order_start_time" if use_start_time else "order_number"
        self.order_key_stats = {
            "distinct_order_number": distinct_order_number,
            "distinct_order_start_time": distinct_start_time,
            "distinct_order_key": int(order_key.nunique(dropna=True)),
        }
        return order_key

    def _load_sales(self) -> pd.DataFrame:
        excel_source = self._read_excel_source()
        try:
            sheet_map = self._read_excel_with_fallback_engines(excel_source)
        except Exception as exc:
            raise RuntimeError(
                f"매출 엑셀을 읽지 못했습니다: {exc}. 암호화 파일이라면 EXCEL_PASSWORD를 확인해 주세요."
            ) from exc
        selected_name, raw_frame = self._select_sheet(sheet_map)
        self.selected_sheet_name = selected_name
        normalized = self._normalize_dataframe(raw_frame)
        if len(normalized) == 0:
            raise RuntimeError("매출 리포트에서 사용할 수 있는 데이터 행을 찾지 못했습니다.")
        return normalized

    def _register_tables(self) -> None:
        self.conn.register("sales_df", self.sales_df)
        self.conn.execute(
            """
            CREATE OR REPLACE TABLE sales AS
            SELECT
                sales_row_id,
                report_name,
                sheet_name,
                branch_name,
                CAST(order_base_date AS TIMESTAMP) AS order_base_date,
                CAST(sales_date AS DATE) AS sales_date,
                sales_year,
                sales_month,
                sales_day,
                CAST(order_start_time AS TIMESTAMP) AS order_start_time,
                sales_hour,
                order_number,
                order_key,
                order_channel,
                payment_status,
                category,
                product_name,
                option_name,
                tax_type,
                quantity,
                product_price,
                option_price,
                product_discount,
                order_discount,
                line_total_amount,
                actual_sales_amount,
                net_sales_amount,
                vat_amount
            FROM sales_df
            """
        )

    def query(self, sql: str) -> pd.DataFrame:
        return self.conn.execute(sql).df()

    def get_full_sales(self) -> pd.DataFrame:
        return self.sales_df.copy()
