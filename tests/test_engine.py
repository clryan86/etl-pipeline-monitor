import sqlite3
from pathlib import Path

from app.store import Store
from etl.engine import PipelineEngine


def test_pipeline_tracks_rows_and_lineage(tmp_path: Path):
    csv = tmp_path / "orders.csv"
    csv.write_text("id,qty,price,name\n1,2,5,A\n1,2,5,A\n2,3,4,B\n3,1,8,\n")
    store = Store(tmp_path / "ops.db")
    pid = store.create_pipeline("Orders", {"tasks": [
        {"type": "extract_csv", "payload_key": "source"},
        {"type": "drop_nulls", "columns": ["name"]},
        {"type": "deduplicate", "columns": ["id"]},
        {"type": "derive", "operation": "multiply", "left": "qty", "right": "price", "column": "revenue"},
        {"type": "quality_gate", "min_rows": 2, "required_columns": ["id", "revenue"]},
        {"type": "load_sqlite", "table": "orders", "mode": "replace"},
    ]})
    run = PipelineEngine(store, tmp_path / "wh.db").run(store.pipeline(pid), {"source": str(csv)})
    assert run["status"] == "SUCCEEDED"
    assert run["rows_in"] == 4
    assert run["rows_out"] == 2
    assert len(run["tasks"]) == 6
    assert run["lineage"][0]["rows_written"] == 2
    with sqlite3.connect(tmp_path / "wh.db") as c:
        assert c.execute("select count(*) from orders").fetchone()[0] == 2


def test_quality_gate_failure_is_persisted(tmp_path: Path):
    csv = tmp_path / "x.csv"
    csv.write_text("id\n1\n")
    store = Store(tmp_path / "ops.db")
    pid = store.create_pipeline("Fail", {"tasks": [
        {"type": "extract_csv", "payload_key": "source"},
        {"type": "quality_gate", "min_rows": 5},
    ]})
    run = PipelineEngine(store, tmp_path / "wh.db").run(store.pipeline(pid), {"source": str(csv)})
    assert run["status"] == "FAILED"
    assert "Quality gate failed" in run["error"]
