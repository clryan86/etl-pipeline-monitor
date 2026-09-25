from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.config import Settings
from app.store import Store
from etl.engine import PipelineEngine

settings = Settings(ops_db=ROOT / "data" / "demo_ops.db", warehouse_db=ROOT / "data" / "demo_wh.db")
store = Store(settings.ops_db)
spec = json.loads((ROOT / "sample_data" / "pipeline.json").read_text())
existing = [p for p in store.pipelines() if p["name"] == spec["name"]]
pid = existing[0]["id"] if existing else store.create_pipeline(spec["name"], {"tasks": spec["tasks"]})
result = PipelineEngine(store, settings.warehouse_db).run(
    store.pipeline(pid), {"source": str(ROOT / "sample_data" / "orders.csv")}
)
print(result["status"], result["rows_in"], result["rows_out"])
