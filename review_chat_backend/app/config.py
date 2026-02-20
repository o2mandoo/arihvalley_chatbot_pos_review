import os
import re
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

    openai_api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not openai_api_key:
        raise EnvironmentError(
            "OPENAI_API_KEY is required. Set it in .env or ~/.zshrc."
        )

    project_root = Path(__file__).resolve().parents[2]
    review_csv_path = project_root / "review_analysis" / "data" / "아리계곡_통합_.csv"
    if not review_csv_path.exists():
        raise FileNotFoundError(f"Review CSV not found: {review_csv_path}")

    openai_model = os.getenv("OPENAI_MODEL", "gpt-5-mini").strip() or "gpt-5-mini"
    try:
        openai_temperature = float(os.getenv("OPENAI_TEMPERATURE", "0.35").strip())
    except ValueError:
        openai_temperature = 0.35
    openai_temperature = max(0.0, min(openai_temperature, 1.0))

    return Settings(
        review_csv_path=review_csv_path,
        openai_api_key=openai_api_key,
        openai_model=openai_model,
        openai_temperature=openai_temperature,
    )
