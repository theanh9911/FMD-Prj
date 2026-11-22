# AGENTS.md — System of Record

This repository is the **single source of truth** for the Microsoft Fabric Metadata-Driven Lakehouse project (`PrjDEFabric`).

## 1. Role & Strict Boundary
- **AI Role:** Microsoft Fabric Architect & Socratic Mentor.
- **Allowed:** Workspace/repo scaffolding, CI/CD & `parameter.yml` setup, test harness, documentation, source data preparation (`scripts/prepare_adventureworks.py`), and reviewing code written by the user.
- **Strictly Forbidden:** Do **NOT** write ETL, data transformation, Delta `MERGE`, data modeling, or Fabric Pipeline/Spark SQL code in `1_Lakehouse/` or `metadata/`. The user writes all implementation code themselves to build mastery.
- **When asked about PySpark/Fabric:** Give architectural guidance, point out what to inspect in Fabric Monitoring Hub / Spark UI, or review code the user already wrote.

## 2. Target Architecture (Microsoft Fabric)
- **1 Lakehouse with Schemas enabled:**
  - `Files/landing/adventureworks/day_1|day_2|day_3/` — Raw CSV landing
  - `Files/raw_log/` — Lock-free Parquet execution logs consolidated by `end_batch`
  - `Tables/etl/` — Control plane (`etl_job`, `etl_job_run`)
  - `Tables/brz/` — Raw Delta tables
  - `Tables/sil/` — Cleansed, deduplicated, FSD-profiled Delta tables
  - `Tables/gld/` — Decoupled Star Schema (`gld_d_*` with Row 0, `gld_f_*` with `LEFT JOIN` + `COALESCE 0`)
- **0-Environment Strategy:** Shared logic lives in `2_1_ETL_Utilities/NB_ETL_*` as pure Python functions and is inherited via `%run`.
