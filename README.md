# ETL Pipeline Monitor

[![CI](https://github.com/clryan86/etl-pipeline-monitor/actions/workflows/ci.yml/badge.svg)](https://github.com/clryan86/etl-pipeline-monitor/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.11%2B-blue)
![ETL](https://img.shields.io/badge/Data-ETL%20Observability-4DB6AC)
![FastAPI](https://img.shields.io/badge/FastAPI-operations-009688)
![Docker](https://img.shields.io/badge/Docker-ready-2496ED)

A production-minded ETL orchestration and observability project that tracks **pipeline runs, task-level row counts, transformations, quality gates, load lineage, failures, timing, and warehouse output**.

## What it demonstrates

- Python data pipelines
- Pandas transformations
- operational metadata and observability
- row-level volume tracking
- quality gates
- source-to-target lineage
- SQLite warehouse loading
- FastAPI operations API
- execution dashboards
- deterministic integration tests
- Docker and GitHub Actions

## Demo pipeline

The included orders pipeline:

1. extracts a CSV feed,
2. removes rows missing a customer,
3. deduplicates order IDs,
4. derives revenue,
5. enforces a minimum-row/schema contract,
6. loads a warehouse fact table,
7. records source-to-target lineage and task metrics.

```bash
pip install -r requirements-dev.txt
python scripts/demo.py
uvicorn app.main:app --reload
```

Open `/docs` for the API or `/` for the operations dashboard.

## Engineering focus

Unlike a single ETL script, the engine preserves operational evidence for each run: status, task order, duration, rows in/out, errors, and lineage. The architecture intentionally separates the pipeline engine from the web layer so the same engine can later be triggered by a scheduler, queue worker, CLI, or orchestration platform.

## Tests

```bash
pytest
ruff check .
```

## Roadmap

- retry/backoff policies per task
- durable checkpoints and incremental loads
- Parquet support
- PostgreSQL/Snowflake targets
- freshness/SLA monitoring
- pipeline scheduling
- schema drift detection
- data contracts and alert destinations
- OpenTelemetry metrics
- queue-backed workers

## Author

**Christopher Ryan**  
Python • Data Engineering • Automation • Analytics

## License

MIT
