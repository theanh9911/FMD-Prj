# PrjDEFabric — Metadata-Driven Microsoft Fabric Lakehouse

Production-aligned Microsoft Fabric repository (FMD v2 architecture) built from scratch using **AdventureWorks** across 1 Lakehouse with Schemas enabled (`etl`, `brz`, `sil`, `gld`).

## Repository Structure (1-to-1 with Fabric Git Integration)

- `0_Deployment/` — CI/CD & environment promotion (`parameter.yml`, deployment scripts)
- `1_Lakehouse/1_Main/` — Fabric Workspace items:
  - `1_Pipeline/` (`1_1_FunctionalPipeline`, `1_2_PipelineRunByLayer`, `1_3_MasterPipeline`)
  - `2_Notebook/` (`2_1_ETL_Utilities`, `2_2_Bronze`, `2_3_Silver`, `2_4_Gold/2_4_1_Dim`, `2_4_Gold/2_4_2_Fact`, `2_99_Adhoc_Check`)
  - `3_Lakehouse/` — Lakehouse & OneLake shortcuts metadata
- `2_Dashboard/` — Direct Lake Semantic Model (`Model/`) & Power BI Report (`Report/`)
- `metadata/` — Control Plane DDL (`ddl/`), job configs (`jobs/`), and contracts (`schemas/`)
- `tests/` — Zero-CU local unit tests (`pytest`) for pure Python functions inside `NB_ETL_*`
- `scripts/` — Source data preparation (`prepare_adventureworks.py`)
- `docs/` — End-to-end curriculum (`curriculum.md`)

## Quick Start on Fabric
1. Create a Fabric Workspace and 1 Lakehouse with **Lakehouse schemas** enabled (`etl`, `brz`, `sil`, `gld`).
2. Upload `.tmp_aw_landing/` (`day_1`, `day_2`, `day_3`) to OneLake `Files/landing/adventureworks/`.
3. Follow [docs/curriculum.md](docs/curriculum.md) phase by phase.
