import os
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


def load_env_from_shell_rc(var_name: str) -> Optional[str]:
    """Load env var from zsh rc files as fallback."""
    candidates = [
        os.path.expanduser("~/.zshrc"),
        os.path.expanduser("~/.zshenv"),
        os.path.expanduser("~/.zprofile"),
    ]
    pattern = re.compile(rf"^(export\s+)?{re.escape(var_name)}\s*=\s*(.+)$")

    for path in candidates:
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as handle:
                for line in handle:
                    stripped = line.strip()
                    if not stripped or stripped.startswith("#"):
                        continue
                    match = pattern.match(stripped)
                    if not match:
                        continue
                    value = match.group(2).strip()
                    if (value.startswith("'") and value.endswith("'")) or (
                        value.startswith('"') and value.endswith('"')
                    ):
                        value = value[1:-1]
                    if value:
                        os.environ[var_name] = value
                        return value
        except OSError:
            continue
    return None


@dataclass(frozen=True)
class Settings:
    review_csv_path: Path
    sales_report_path: Optional[Path]
    sales_excel_password: Optional[str]
    sales_branch_name: str
    openai_api_key: str
    openai_model: str
    openai_temperature: float
    max_sql_rows: int = 200
    max_table_rows: int = 20


def get_settings() -> Settings:
    load_dotenv()

    if not os.getenv("OPENAI_API_KEY"):
        load_env_from_shell_rc("OPENAI_API_KEY")
    if not os.getenv("OPENAI_MODEL"):
        load_env_from_shell_rc("OPENAI_MODEL")
    if not os.getenv("EXCEL_PASSWORD"):
        load_env_from_shell_rc("EXCEL_PASSWORD")

    openai_api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not openai_api_key:
        raise EnvironmentError(
            "OPENAI_API_KEY is required. Set it in .env or ~/.zshrc."
        )

    project_root = Path(__file__).resolve().parents[2]
    review_csv_path = project_root / "review_analysis" / "data" / "아리계곡_통합_.csv"
    if not review_csv_path.exists():
        raise FileNotFoundError(f"Review CSV not found: {review_csv_path}")

    sales_report_name = (
        os.getenv("SALES_REPORT_FILE", "왕십리_매출리포트-260221.xlsx").strip()
        or "왕십리_매출리포트-260221.xlsx"
    )
    sales_report_path: Optional[Path] = None
    revenue_dir = project_root / "revenue-data"
    if revenue_dir.exists():
        candidates = [
            path
            for path in revenue_dir.glob("*.xlsx")
            if not path.name.startswith("~$")
        ]
        normalized_target = unicodedata.normalize("NFC", sales_report_name)
        matched = [
            path
            for path in candidates
            if unicodedata.normalize("NFC", path.name) == normalized_target
        ]
        if matched:
            sales_report_path = matched[0]
        else:
            non_decrypted = [
                path
                for path in candidates
                if "-decrypted" not in unicodedata.normalize("NFC", path.name)
            ]
            fallback_pool = non_decrypted or candidates
            fallback_pool = sorted(
                fallback_pool,
                key=lambda path: path.stat().st_mtime,
                reverse=True,
            )
            sales_report_path = fallback_pool[0] if fallback_pool else None

    sales_excel_password_raw = os.getenv("EXCEL_PASSWORD", "").strip()
    if not sales_excel_password_raw:
        sales_excel_password_raw = "7055"
    sales_excel_password = sales_excel_password_raw
    sales_branch_name = os.getenv("SALES_BRANCH_NAME", "왕십리한양대점").strip() or "왕십리한양대점"

    openai_model = os.getenv("OPENAI_MODEL", "gpt-5-mini").strip() or "gpt-5-mini"
    try:
        openai_temperature = float(os.getenv("OPENAI_TEMPERATURE", "0.45").strip())
    except ValueError:
        openai_temperature = 0.45
    openai_temperature = max(0.0, min(openai_temperature, 1.0))

    return Settings(
        review_csv_path=review_csv_path,
        sales_report_path=sales_report_path,
        sales_excel_password=sales_excel_password,
        sales_branch_name=sales_branch_name,
        openai_api_key=openai_api_key,
        openai_model=openai_model,
        openai_temperature=openai_temperature,
    )
