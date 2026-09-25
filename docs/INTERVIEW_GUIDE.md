# Interview Guide

## 30-second explanation

> ETL Pipeline Monitor is a small orchestration and observability system. A pipeline definition describes extraction, cleaning, derivation, validation and load steps. Every run persists task timing, rows in/out, errors and source-to-target lineage, so it demonstrates the operational side of data engineering rather than only dataframe transformations.

## Key design choices

**Why separate the warehouse and ops database?** Pipeline metadata has a different lifecycle and query pattern from analytical business data. Separation also mirrors production systems where orchestration metadata and a warehouse are distinct services.

**Why quality gates before load?** Bad data should fail before publication. A gate can prevent incomplete or structurally invalid data from reaching downstream consumers.

**Why capture row counts at every task?** Row-count deltas make filtering, data loss and unexpected explosions visible during incident review.

## Resume bullets

- Built a Python ETL engine with extraction, cleaning, deduplication, derived fields, quality gates, and SQLite warehouse loading.
- Persisted task-level row counts, durations, failures, and source-to-target lineage for pipeline observability and auditability.
- Added a FastAPI operations layer, dashboard, integration tests, Docker packaging, CI, and production-evolution documentation.
