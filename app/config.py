from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    ops_db: Path
    warehouse_db: Path
    app_name: str = "ETL Pipeline Monitor"

    @classmethod
    def from_env(cls) -> "Settings":
        root = Path(__file__).resolve().parents[1]
        return cls(
            ops_db=Path(os.getenv("OPS_DB", root / "data" / "ops.db")),
            warehouse_db=Path(os.getenv("WAREHOUSE_DB", root / "data" / "warehouse.db")),
        )
