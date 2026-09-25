from pathlib import Path

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


def test_api_pipeline_flow(tmp_path: Path):
    csv = tmp_path / "data.csv"
    csv.write_text("id,value\n1,10\n2,20\n")
    c = TestClient(create_app(Settings(tmp_path / "ops.db", tmp_path / "wh.db")))
    created = c.post("/api/pipelines", json={
        "name": "Simple load",
        "tasks": [
            {"type": "extract_csv", "payload_key": "source"},
            {"type": "quality_gate", "min_rows": 2, "required_columns": ["id"]},
            {"type": "load_sqlite", "table": "facts", "mode": "replace"},
        ],
    })
    assert created.status_code == 201
    run = c.post(
        f"/api/pipelines/{created.json()['id']}/run",
        json={"payload": {"source": str(csv)}},
    )
    assert run.status_code == 200
    assert run.json()["status"] == "SUCCEEDED"
    metrics = c.get("/api/metrics").json()
    assert metrics["runs"] == 1
    assert metrics["rows_out"] == 2
