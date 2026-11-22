# Project Status

## Current Phase: Ready for Phase 1 on Microsoft Fabric
- [x] Local Docker stack stopped & cleaned (`PrjDEFabric` restructured 1-to-1 with Fabric Git production layout)
- [x] AdventureWorks 3-day batches (`day_1`, `day_2`, `day_3` with edge cases) prepared in `.tmp_aw_landing/` via `scripts/prepare_adventureworks.py`
- [x] End-to-end curriculum aligned to Fabric production (`docs/curriculum.md`)

## Next Step
1. Upload `.tmp_aw_landing/` to Fabric Lakehouse `Files/landing/adventureworks/`.
2. Connect Fabric Workspace to Git (optional) and start **Phase 1** (`NB_ETL_DATA_OPS` & `NB_ETL_DATA_QUANTITY`).
