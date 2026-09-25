from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any

import pandas as pd

from app.store import Store


class PipelineError(RuntimeError):
    pass


class PipelineEngine:
    def __init__(self, store: Store, warehouse_db: Path):
        self.store = store
        self.warehouse_db = Path(warehouse_db)
        self.warehouse_db.parent.mkdir(parents=True, exist_ok=True)

    def run(self, pipeline: dict[str, Any], payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        run_id = self.store.create_run(pipeline["id"], payload)
        started = time.perf_counter()
        frame = None
        rows_in = 0
        rows_out = 0
        source = "unknown"
        try:
            for position, task in enumerate(pipeline["definition"].get("tasks", [])):
                frame, meta = self._task(frame, task, payload)
                rows_in = max(rows_in, int(meta.get("rows_in", 0)))
                rows_out = int(meta.get("rows_out", rows_out))
                self.store.add_task(
                    run_id, position, task.get("name", task["type"]), task["type"],
                    "SUCCEEDED", 1, int(meta.get("rows_in", 0)), int(meta.get("rows_out", 0)),
                    float(meta.get("duration_ms", 0)), None, meta,
                )
                if task["type"] == "extract_csv":
                    source = meta["source"]
                if task["type"] == "load_sqlite":
                    self.store.add_lineage(run_id, source, meta["target"], meta["rows_out"])
            duration = (time.perf_counter() - started) * 1000
            self.store.finish_run(run_id, "SUCCEEDED", rows_in, rows_out, duration)
            return self.store.run(run_id)
        except Exception as exc:
            duration = (time.perf_counter() - started) * 1000
            self.store.finish_run(run_id, "FAILED", rows_in, rows_out, duration, str(exc))
            return self.store.run(run_id)

    def _task(self, frame: pd.DataFrame | None, task: dict[str, Any], payload: dict[str, Any]):
        start = time.perf_counter()
        kind = task["type"]
        if kind == "extract_csv":
            path = Path(payload.get(task.get("payload_key", "source"), task.get("path", "")))
            frame = pd.read_csv(path)
            meta = {"source": str(path), "rows_in": len(frame), "rows_out": len(frame)}
        elif kind == "drop_nulls":
            self._need(frame)
            before = len(frame)
            frame = frame.dropna(subset=task.get("columns"))
            meta = {"rows_in": before, "rows_out": len(frame), "dropped": before - len(frame)}
        elif kind == "deduplicate":
            self._need(frame)
            before = len(frame)
            frame = frame.drop_duplicates(subset=task.get("columns"))
            meta = {"rows_in": before, "rows_out": len(frame), "dropped": before - len(frame)}
        elif kind == "derive":
            self._need(frame)
            column = task["column"]
            operation = task["operation"]
            if operation == "multiply":
                frame[column] = pd.to_numeric(frame[task["left"]]) * pd.to_numeric(frame[task["right"]])
            elif operation == "lower":
                frame[column] = frame[task["source"]].astype(str).str.lower()
            else:
                raise PipelineError(f"Unsupported derive operation: {operation}")
            meta = {"rows_in": len(frame), "rows_out": len(frame), "column": column}
        elif kind == "quality_gate":
            self._need(frame)
            minimum = int(task.get("min_rows", 1))
            required = task.get("required_columns", [])
            missing = [c for c in required if c not in frame.columns]
            if len(frame) < minimum:
                raise PipelineError(f"Quality gate failed: {len(frame)} rows < {minimum}")
            if missing:
                raise PipelineError(f"Quality gate failed: missing columns {missing}")
            meta = {"rows_in": len(frame), "rows_out": len(frame), "required_columns": required}
        elif kind == "load_sqlite":
            self._need(frame)
            table = task["table"]
            mode = task.get("mode", "replace")
            with sqlite3.connect(self.warehouse_db) as conn:
                frame.to_sql(table, conn, if_exists=mode, index=False)
            meta = {
                "rows_in": len(frame), "rows_out": len(frame),
                "target": f"sqlite://{self.warehouse_db}#{table}", "table": table,
            }
        else:
            raise PipelineError(f"Unsupported task: {kind}")
        meta["duration_ms"] = round((time.perf_counter() - start) * 1000, 3)
        return frame, meta

    @staticmethod
    def _need(frame):
        if frame is None:
            raise PipelineError("Task requires a dataframe")
