from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from etl.engine import PipelineEngine

from .config import Settings
from .store import Store

ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = Jinja2Templates(directory=str(ROOT / "templates"))


class PipelineCreate(BaseModel):
    name: str = Field(min_length=3)
    tasks: list[dict] = Field(min_length=1)


class RunRequest(BaseModel):
    payload: dict = {}


def create_app(settings: Settings | None = None):
    settings = settings or Settings.from_env()
    store = Store(settings.ops_db)
    engine = PipelineEngine(store, settings.warehouse_db)
    app = FastAPI(title=settings.app_name, version="1.0.0")
    app.state.store = store
    app.state.engine = engine
    app.mount("/static", StaticFiles(directory=str(ROOT / "static")), name="static")

    @app.get("/healthz")
    def health():
        return {"ok": True, "service": settings.app_name}

    @app.get("/", response_class=HTMLResponse)
    def home(request: Request):
        pipelines = store.pipelines()
        runs = store.runs(20)
        ok = sum(1 for r in runs if r["status"] == "SUCCEEDED")
        metrics = {
            "pipelines": len(pipelines),
            "runs": len(store.runs(1000)),
            "success_rate": round(ok / len(runs) * 100, 1) if runs else 0,
            "rows_loaded": sum(r["rows_out"] for r in runs),
        }
        return TEMPLATES.TemplateResponse(
            request=request,
            name="index.html",
            context={"app_name": settings.app_name, "pipelines": pipelines, "runs": runs, "metrics": metrics},
        )

    @app.get("/runs/{run_id}", response_class=HTMLResponse)
    def run_page(run_id: int, request: Request):
        run = store.run(run_id)
        if not run:
            raise HTTPException(404, "Run not found")
        return TEMPLATES.TemplateResponse(
            request=request, name="run.html", context={"app_name": settings.app_name, "run": run}
        )

    @app.post("/api/pipelines", status_code=201)
    def create_pipeline(p: PipelineCreate):
        try:
            pid = store.create_pipeline(p.name, {"tasks": p.tasks})
        except Exception as exc:
            raise HTTPException(409, "Pipeline name exists") from exc
        return store.pipeline(pid)

    @app.get("/api/pipelines")
    def pipelines():
        return store.pipelines()

    @app.post("/api/pipelines/{pipeline_id}/run")
    def run_pipeline(pipeline_id: int, req: RunRequest):
        p = store.pipeline(pipeline_id)
        if not p:
            raise HTTPException(404, "Pipeline not found")
        return engine.run(p, req.payload)

    @app.get("/api/runs")
    def runs(limit: int = 50):
        return store.runs(max(1, min(limit, 200)))

    @app.get("/api/runs/{run_id}")
    def run(run_id: int):
        r = store.run(run_id)
        if not r:
            raise HTTPException(404, "Run not found")
        return r

    @app.get("/api/metrics")
    def metrics():
        runs = store.runs(1000)
        return {
            "runs": len(runs),
            "successful": sum(r["status"] == "SUCCEEDED" for r in runs),
            "failed": sum(r["status"] == "FAILED" for r in runs),
            "rows_in": sum(r["rows_in"] for r in runs),
            "rows_out": sum(r["rows_out"] for r in runs),
        }

    return app


app = create_app()
