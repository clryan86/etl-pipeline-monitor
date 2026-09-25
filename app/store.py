from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS pipelines(
 id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL, definition_json TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runs(
 id INTEGER PRIMARY KEY AUTOINCREMENT, pipeline_id INTEGER NOT NULL, status TEXT NOT NULL, input_json TEXT NOT NULL,
 rows_in INTEGER NOT NULL DEFAULT 0, rows_out INTEGER NOT NULL DEFAULT 0, error TEXT, started_at TEXT NOT NULL,
 finished_at TEXT, duration_ms REAL, FOREIGN KEY(pipeline_id) REFERENCES pipelines(id)
);
CREATE TABLE IF NOT EXISTS task_runs(
 id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER NOT NULL, position INTEGER NOT NULL, task_name TEXT NOT NULL,
 task_type TEXT NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL, rows_in INTEGER NOT NULL DEFAULT 0,
 rows_out INTEGER NOT NULL DEFAULT 0, duration_ms REAL, error TEXT, metadata_json TEXT NOT NULL DEFAULT '{}',
 FOREIGN KEY(run_id) REFERENCES runs(id)
);
CREATE TABLE IF NOT EXISTS lineage(
 id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER NOT NULL, source_name TEXT NOT NULL, target_name TEXT NOT NULL,
 rows_written INTEGER NOT NULL, created_at TEXT NOT NULL, FOREIGN KEY(run_id) REFERENCES runs(id)
);
CREATE TABLE IF NOT EXISTS checkpoints(
 pipeline_id INTEGER NOT NULL, checkpoint_key TEXT NOT NULL, checkpoint_value TEXT NOT NULL, updated_at TEXT NOT NULL,
 PRIMARY KEY(pipeline_id, checkpoint_key)
);
"""


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Store:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as c:
            c.executescript(SCHEMA)

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        c = sqlite3.connect(self.path)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA foreign_keys=ON")
        try:
            yield c
            c.commit()
        finally:
            c.close()

    def create_pipeline(self, name: str, definition: dict[str, Any]) -> int:
        with self.connection() as c:
            cur = c.execute(
                "INSERT INTO pipelines(name,definition_json,created_at) VALUES(?,?,?)",
                (name, json.dumps(definition), now()),
            )
            return int(cur.lastrowid)

    def pipelines(self):
        with self.connection() as c:
            rows = c.execute("SELECT * FROM pipelines ORDER BY id DESC").fetchall()
        return [self._pipeline(r) for r in rows]

    def pipeline(self, pipeline_id: int):
        with self.connection() as c:
            r = c.execute("SELECT * FROM pipelines WHERE id=?", (pipeline_id,)).fetchone()
        return self._pipeline(r) if r else None

    def create_run(self, pipeline_id: int, payload: dict[str, Any]) -> int:
        with self.connection() as c:
            cur = c.execute(
                "INSERT INTO runs(pipeline_id,status,input_json,started_at) VALUES(?,?,?,?)",
                (pipeline_id, "RUNNING", json.dumps(payload), now()),
            )
            return int(cur.lastrowid)

    def finish_run(self, run_id: int, status: str, rows_in: int, rows_out: int, duration_ms: float, error: str | None = None):
        with self.connection() as c:
            c.execute(
                "UPDATE runs SET status=?,rows_in=?,rows_out=?,duration_ms=?,error=?,finished_at=? WHERE id=?",
                (status, rows_in, rows_out, duration_ms, error, now(), run_id),
            )

    def add_task(self, run_id: int, position: int, name: str, kind: str, status: str, attempts: int, rows_in: int, rows_out: int, duration_ms: float, error: str | None, metadata: dict[str, Any]):
        with self.connection() as c:
            c.execute(
                "INSERT INTO task_runs(run_id,position,task_name,task_type,status,attempts,rows_in,rows_out,duration_ms,error,metadata_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (run_id, position, name, kind, status, attempts, rows_in, rows_out, duration_ms, error, json.dumps(metadata)),
            )

    def add_lineage(self, run_id: int, source: str, target: str, rows: int):
        with self.connection() as c:
            c.execute(
                "INSERT INTO lineage(run_id,source_name,target_name,rows_written,created_at) VALUES(?,?,?,?,?)",
                (run_id, source, target, rows, now()),
            )

    def set_checkpoint(self, pipeline_id: int, key: str, value: str):
        with self.connection() as c:
            c.execute(
                "INSERT INTO checkpoints(pipeline_id,checkpoint_key,checkpoint_value,updated_at) VALUES(?,?,?,?) "
                "ON CONFLICT(pipeline_id,checkpoint_key) DO UPDATE SET checkpoint_value=excluded.checkpoint_value,updated_at=excluded.updated_at",
                (pipeline_id, key, value, now()),
            )

    def runs(self, limit: int = 50):
        with self.connection() as c:
            rows = c.execute(
                "SELECT r.*,p.name pipeline_name FROM runs r JOIN pipelines p ON p.id=r.pipeline_id ORDER BY r.id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]

    def run(self, run_id: int):
        with self.connection() as c:
            r = c.execute(
                "SELECT r.*,p.name pipeline_name FROM runs r JOIN pipelines p ON p.id=r.pipeline_id WHERE r.id=?",
                (run_id,),
            ).fetchone()
            if not r:
                return None
            tasks = c.execute("SELECT * FROM task_runs WHERE run_id=? ORDER BY position", (run_id,)).fetchall()
            lineage = c.execute("SELECT * FROM lineage WHERE run_id=? ORDER BY id", (run_id,)).fetchall()
        out = dict(r)
        out["tasks"] = [{**dict(t), "metadata": json.loads(t["metadata_json"])} for t in tasks]
        out["lineage"] = [dict(x) for x in lineage]
        return out

    @staticmethod
    def _pipeline(r):
        x = dict(r)
        x["definition"] = json.loads(x.pop("definition_json"))
        return x
