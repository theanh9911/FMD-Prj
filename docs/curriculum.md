# Giáo Trình Thực Chiến: Metadata-Driven Lakehouse End-to-End (Local → Microsoft Fabric)

> **Mục tiêu:** Tự tay xây dựng từ đầu (from scratch) một hệ thống Data Lakehouse chuẩn Production theo kiến trúc **Metadata-Driven (FMD v2)** — thiết kế để chịu tải **1.000+ bảng, đa lịch chạy (multi-schedule), tách rời vòng đời Fact/Dim**, luyện tập trên 7 bảng **AdventureWorks** ở môi trường Docker local trước khi đưa lên Microsoft Fabric.
>
> **Nguyên tắc học:** Giáo trình cung cấp **kiến trúc, contract, edge-cases và bài kiểm chứng**. Bạn tự tay gõ toàn bộ code ETL trong `src/`, `metadata/`, `notebooks/`, `tests/`.

---

## 0. Bản Đồ Kiến Trúc & Dataset Thực Hành

### 0.1. Sơ Đồ 1: Kiến Trúc Tổng Thể (Multi-Schedule, Control Plane & Batch Lifecycle)

```text
  [1] TRIGGERS (Multi-Schedule via BATCH_GROUP)
  +------------------+    +------------------+    +------------------+
  | BKD30MIN (30m)   |    | DAILY (02:00 AM) |    | MANUAL_DIM       |
  +--------+---------+    +--------+---------+    +--------+---------+
           |                       |                       |
           +-----------------------+-----------------------+
                                   |
                                   v
  [2] CONTROL PLANE & ORCHESTRATOR (etl_load_metadata + etl_caller)
  +-------------------------------------------------------------------+
  | 1. START_BATCH: Sinh batch_id, load config từ etl.etl_job         |
  | 2. FILTER: Lọc các job ACTIVE_FLG='Y' khớp token BATCH_GROUP      |
  | 3. DAG ENGINE: Kahn's Topo Sort (chặn Cycle) + Check Dependency   |
  | 4. DISPATCH: Chạy song song các node sẵn sàng (Ready-Queue)       |
  |    * Nếu Cha FAILED -> Con tự động đánh SKIPPED_UPSTREAM_FAILED   |
  | 5. END_BATCH: Gom raw_log/*.parquet -> APPEND vào etl.etl_job_run |
  |               Cập nhật LAST_WATERMARK_VALUE -> Gửi Alert nếu fail |
  +--------------------------------+----------------------------------+
                                   |
                                   v
  [3] DATA PLANE (MinIO / OneLake + Hive Metastore / Fabric Schemas)
  +-------------------+   +-------------------+   +-------------------+
  | Files/landing/    |   | BRONZE (brz.*)    |   | SILVER (sil.*)    |
  | - day_1/ (Init)   |-->| nb_brz_template   |-->| nb_sil_generic    |
  | - day_2/ (Dirty)  |   | - Raw + Audit col |   | - Watermark+Prune |
  | - day_3/ (Recov)  |   | - Schema-on-read  |   | - Dedup + 5 DQ    |
  +-------------------+   +-------------------+   | - SCD1/SCD2/SoftD |
                                                  +---------+---------+
                                                            |
        +---------------------------------------------------+
        |  (Decoupled Fact & Dim: chạy độc lập theo lịch)
        v
  +-------------------------------------------------------------------+
  | GOLD (gld.*) — Star Schema sẵn sàng cho Power BI / Direct Lake    |
  |                                                                   |
  |  [nb_gld_d_customer / d_product / d_date]  <-- (Chạy DAILY/MANUAL)|
  |     -> Luôn có Type-Safe Row 0 (ROW_ID = 0: "Không xác định")     |
  |                                                                   |
  |  [nb_gld_f_sales]                          <-- (Chạy BKD30MIN)    |
  |     -> LEFT JOIN Dim + COALESCE(ROW_ID, 0) (Không bao giờ rớt số) |
  |     -> PARTITION_OVERWRITE (PRUNE_DAY=-20) (Tự vá Row 0 khi Dim   |
  |                                             chạy bù sau đó)       |
  +-------------------------------------------------------------------+
```

### 0.2. Sơ Đồ 2: Luồng Thực Thi Bên Trong 1 Job (7-Cell Contract + 5 DQ Gates)

```text
  [Params từ etl_caller / papermill]
                 |
                 v
  (Cell 3) build_context() ──> Gộp params + parse EXTRA_CONFIG_JSON
                 |
                 v
  (Cell 4-5) transform_data() ──> Đọc nguồn (Watermark + PRUNE_DAY)
                 |                Khử trùng theo CHECK_DUP_KEY (Window)
                 |                Transform nghiệp vụ (nếu ở Gold)
                 v
  (Cell 6) reconcile_job() ──> Gắn FSD Technical Columns (_etl_*, _valid_*)
                 |             Chạy 5 Data Quality Gates:
                 |               G1: Schema Contract (đủ cột, đúng kiểu)
                 |               G2: Key Uniqueness (0 duplicate key)
                 |               G3: Key Completeness (0 NULL key)
                 |               G4: Business Rules (min_rows, range bounds)
                 |               G5: Reconciliation (src == tgt + rejected)
                 |
                 +---> [NẾU VI PHẠM G1..G5]
                 |       |-- dq_mode = FAIL_FAST  --> Raise Error -> FAILED
                 |       |-- dq_mode = QUARANTINE --> Đẩy dòng lỗi sang
                 |                                    silver._quarantine_*
                 v
  (Cell 7) write_by_profile() ──> OVERWRITE / APPEND / PARTITION_OVERWRITE
                 |                SCD1 (+ Soft Delete) / SCD2 (Hash + Latch)
                 v
  [Ghi lock-free raw_log/<batch_id>/<job_name>.parquet + Update Watermark]
```

### 0.3. Kiến Trúc Thư Mục Dự Án (`PrjDEFabric` ↔ Fabric Git Integration)

Cấu trúc chuẩn Production map 1-1 với Microsoft Fabric Workspace:

```text
PrjDEFabric/
├── 0_Deployment/                      # CI/CD, parameter.yml (Dev -> Test -> Prod)
├── 1_Lakehouse/
│   └── 1_Main/
│       ├── 1_Pipeline/                # Fabric Data Pipelines (3 lớp)
│       │   ├── 1_1_FunctionalPipeline/    # PL_FUNC_START_BATCH, END_BATCH, RUN_NOTEBOOK_CALLER, SEND_NOTIFICATION
│       │   ├── 1_2_PipelineRunByLayer/    # PL_LAYER_SRC2BRZ, PL_LAYER_BRZ2SIL, PL_LAYER_SIL2GLD
│       │   └── 1_3_MasterPipeline/        # PL_MASTER_ORCHESTRATOR, PL_MASTER_RECONCILE
│       ├── 2_Notebook/                # 0-Environment Strategy (%run + Pure Functions)
│       │   ├── 2_1_ETL_Utilities/         # NB_ETL_DATA_OPS, NB_ETL_DATA_QUANTITY, NB_ETL_LOAD_METADATA_V2, NB_ETL_CALLER
│       │   ├── 2_2_Bronze/                # NB_BRZ_TEMPLATE (1 notebook cho mọi bảng)
│       │   ├── 2_3_Silver/                # NB_SIL_GENERIC (1 notebook cho mọi bảng)
│       │   ├── 2_4_Gold/
│       │   │   ├── 2_4_1_Dim/             # NB_GLD_D_CUSTOMER, NB_GLD_D_PRODUCT, NB_GLD_D_DATE (có Row 0)
│       │   │   └── 2_4_2_Fact/            # NB_GLD_F_SALES (LEFT JOIN + COALESCE 0 + PRUNE_DAY)
│       │   └── 2_99_Adhoc_Check/          # Sandbox ad-hoc / kiểm tra dữ liệu
│       └── 3_Lakehouse/               # Lakehouse metadata & shortcuts.metadata.json
├── 2_Dashboard/
│   ├── Model/                         # Direct Lake Semantic Model
│   └── Report/                        # Power BI Report (PBIP)
├── metadata/
│   ├── ddl/                           # etl_tables.sql (etl, brz, sil, gld)
│   ├── jobs/                          # etl_jobs.json
│   └── schemas/                       # etl_job.schema.json
├── scripts/
│   └── prepare_adventureworks.py      # Đã chuẩn bị sẵn 3 batch trong .tmp_aw_landing/
├── tests/                             # Unit test pure Python (pytest, chạy 0-CU ở local)
└── docs/
    └── curriculum.md                  # Giáo trình này
```

### 0.4. 7 Bảng AdventureWorks (Snowflake OLTP → Star Schema Gold)

| # | Bảng Source | Layer Silver (`LOAD_TYPE`) | Tầng Gold | Lịch (`BATCH_GROUP`) | Case kỹ thuật bắt buộc luyện |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | `SalesOrderHeader` | `PARTITION_OVERWRITE` (`OrderDate`, `PRUNE_DAY=-7`) | `gld.gld_f_sales` | `BKD30MIN\|DAILY` | Watermark (`ModifiedDate`), Late-arriving update, Duplicate `SalesOrderID`, Dirty row (`TotalDue < 0`). |
| 2 | `SalesOrderDetail` | `SCD1` (`SalesOrderID\|SalesOrderDetailID`) | `gld.gld_f_sales` | `BKD30MIN\|DAILY` | Composite integration key, Reconcile tổng `LineTotal` vs `SubTotal`. |
| 3 | `Customer` | `SCD2` (`CustomerID`) | `gld.gld_d_customer` | `DAILY\|MANUAL_DIM` | Đổi `CompanyName`/`EmailAddress`/`Phone` → đóng dòng cũ, mở dòng mới (`_valid_from`, `_valid_to`, `_is_current`). |
| 4 | `CustomerAddress` | `SCD1` (`CustomerID\|AddressID`) | `gld.gld_d_customer` | `DAILY\|MANUAL_DIM` | Bảng cầu nối Snowflake (1-N), chọn địa chỉ chính (`AddressType = 'Main Office'`). |
| 5 | `Address` | `SCD1` (`AddressID`) | `gld.gld_d_customer` | `DAILY\|MANUAL_DIM` | Join 3 bảng Snowflake (`Customer` + `CustomerAddress` + `Address`) thành 1 Dim phẳng. |
| 6 | `Product` | `SCD1` + `ENABLE_SOFT_DELETE=True` | `gld.gld_d_product` | `DAILY` | Snapshot thiếu 1 sản phẩm ở `day_2` → tự đánh dấu `_is_deleted = true`. |
| 7 | `ProductCategory` | `OVERWRITE` | `gld.gld_d_product` | `DAILY` | Bảng nhỏ → **Broadcast Join** khi ghép vào `gld_d_product`; xử lý hierarchy cha/con (`ParentProductCategoryID`). |
| *(Tự sinh)* | — | — | `gld.gld_d_date` | `DAILY` | Tự sinh lịch 2000–2030 bằng PySpark + Row 0. |

---

## Phase 0: Hạ Tầng & Source Giả Lập

### Mục tiêu
Đảm bảo Docker Lakehouse chạy ổn định và có sẵn dữ liệu AdventureWorks chia làm 3 ngày (`day_1`, `day_2`, `day_3`) trong MinIO `Files/landing/`.

### Việc cần làm
1. Mở terminal tại `d:\Project\PrjDEFabric`, đồng bộ môi trường và bật stack:
   ```powershell
   uv sync
   docker compose up -d
   docker compose exec jupyter python /workspace/scripts/smoke_test.py
   ```
2. Kiểm tra truy cập các UI:
   - JupyterLab: `http://localhost:8888`
   - MinIO Console: `http://localhost:9001` (`minioadmin` / `minioadmin`)
   - Spark Master / History Server: `http://localhost:8080` / `http://localhost:18080`
3. Chạy script chuẩn bị dữ liệu giả lập `scripts/prepare_adventureworks.py` để đẩy 3 thư mục `day_1/`, `day_2/`, `day_3/` vào `s3a://lakehouse/Files/landing/adventureworks/`.

### Definition of Done (DoD)
- [ ] `smoke_test.py` in ra `Smoke test PASSED`.
- [ ] Trên MinIO Console thấy đủ 7 file CSV cho mỗi thư mục `day_1`, `day_2`, `day_3`.

---

## Phase 1: Build Core Framework Engine (`src/etl_data_ops.py`, `src/etl_data_quality.py`, `tests/`)

### Tư duy kiến trúc (Build từ gốc lên ngọn)
Chưa cần bảng metadata hay pipeline vội — trước hết bạn phải tự tay xây được **trái tim của framework**: các hàm pure Python xử lý FSD profile, hashing, dedup, ghi Delta (`OVERWRITE`, `PARTITION_OVERWRITE`, `SCD1`, `SCD2`) và 5 cổng Data Quality. Viết đến đâu chạy `pytest` kiểm chứng ngay đến đó ở 0 cost.

### Yêu cầu tự code
1. **`src/etl_data_ops.py`**:
   - **4 FSD Technical Profiles (`apply_fsd_profile`):**
     - Profile 1 (`OVERWRITE`, `APPEND`, `PARTITION_OVERWRITE`): chỉ thêm `_etl_loaded_at`, `_etl_batch_id` (không tính hash để tiết kiệm CPU cho bảng Fact lớn).
     - Profile 2 (`SCD1` không soft-delete): `_etl_created_at`, `_etl_updated_at`, `_etl_row_hash`, `_etl_batch_id`.
     - Profile 3 (`SCD1` có soft-delete): Profile 2 + `_is_deleted`, `_etl_deleted_at`.
     - Profile 4 (`SCD2`): `_valid_from`, `_valid_to` (`9999-12-31 23:59:59`), `_is_current`, `_etl_row_hash`, `_etl_batch_id`, `_etl_loaded_at`.
   - **Collision-proof Hashing (`add_row_hash`):** Loại bỏ các cột kỹ thuật (`_etl_*`, `_valid_*`, `_is_*`), sắp xếp tên cột theo alphabet, dùng `sha2(to_json(struct(*sorted_cols)), 256)`. *(Thử giải thích: tại sao `to_json(struct(...))` an toàn hơn `concat_ws('|', ...)`?)*
   - **Deduplication (`dedup_source`):** Dùng `Window.partitionBy(CHECK_DUP_KEY).orderBy(WATERMARK_COLUMN.desc())`, giữ `row_number() == 1`.
   - **Type-Safe Row 0 (`generate_type_safe_row_zero`):** Tạo 1 dòng mặc định (`ROW_ID = 0`, string `"Không xác định"`, số `0`, ngày `1900-01-01`) khớp đúng kiểu dữ liệu schema đích.
   - **Generic Delta Writer (`write_by_profile`):**
     - `OVERWRITE` / `APPEND`
     - `PARTITION_OVERWRITE`: lấy danh sách partition có trong batch hiện tại, dùng `.option("replaceWhere", ...)` để chỉ ghi đè đúng các ngày/tháng bị ảnh hưởng.
     - `SCD1`: `MERGE INTO` dùng null-safe `<=>` trên `INTEGRATION_KEY`, chỉ update khi `NOT (tgt._etl_row_hash <=> src._etl_row_hash)`. Nếu `ENABLE_SOFT_DELETE = True` (cho bảng Snapshot): dòng nào có trong target (`_is_deleted = false`) nhưng không có trong source mới → update `_is_deleted = true`, `_etl_deleted_at = current_timestamp()`.
     - `SCD2`:
       - Bước 1 (`MERGE`): đóng các dòng đang active (`_is_current = true`) có key trùng nhưng khác `_etl_row_hash` **và** thỏa điều kiện out-of-order latching (`src_ts >= tgt._valid_from`).
       - Bước 2 (`INSERT`): chèn version mới cho các key mới tinh hoặc key vừa bị đóng ở bước 1.

2. **`src/etl_data_quality.py`**:
   - Tự code 5 DQ Gates (`run_all_dq_gates`):
     - `G1_SCHEMA_CONTRACT`: đủ cột bắt buộc & đúng kiểu dữ liệu.
     - `G2_KEY_UNIQUENESS`: không còn trùng `INTEGRATION_KEY` sau bước dedup.
     - `G3_KEY_COMPLETENESS`: `INTEGRATION_KEY` không chứa `NULL`.
     - `G4_BUSINESS_RULES`: kiểm tra `dq_min_rows` và `dq_ranges` (ví dụ `TotalDue >= 0`) từ `EXTRA_CONFIG_JSON`. Hỗ trợ tách dòng vi phạm ra bảng/thư mục quarantine và trả về `rejected_row_count` nếu cấu hình `dq_mode = QUARANTINE`, hoặc ném `DataQualityError` nếu `FAIL_FAST`.
     - `G5_RECONCILIATION`: đối soát `abs(src_count - (tgt_count + rejected_count)) <= tolerance_pct`.

3. **`tests/`**:
   - Viết unit test bằng `pytest` kiểm tra: `add_row_hash` không đổi khi đảo thứ tự cột, `dedup_source` giữ bản ghi mới nhất, `SCD2` chạy lại cùng batch 2 lần không tạo duplicate version, và 5 DQ gates bắt đúng lỗi.

### Definition of Done (DoD)
- [ ] `pytest tests/` pass 100%.
- [ ] Tự giải thích được vì sao `write_by_profile` phải kiểm tra `NOT (tgt._etl_row_hash <=> src._etl_row_hash)` trước khi `UPDATE` trong `SCD1`/`SCD2`.

---

## Phase 2: Generic Medallion Notebooks & Decoupled Fact/Dim (`notebooks/`)

### Tư duy kiến trúc: Chuẩn 7-Cell Contract (Chạy tay theo tham số trước)
Lúc này **chưa cần bảng `etl.etl_job`**. Bạn lắp Core Engine ở Phase 1 vào các notebook theo chuẩn 7-Cell Contract và truyền tham số trực tiếp trong `PARAMETERS CELL` để chạy thử trên 7 bảng AdventureWorks (`day_1` → `day_2` → `day_3`):
1. Import helper từ `src/` (sau này lên Fabric đổi thành `%run`)
2. `PARAMETERS CELL` (nhận tham số `papermill`: `BATCH_ID`, `JOB_NAME`, `LOAD_TYPE`, `PRUNE_DAY`...)
3. `build_context()` (gom params + parse `EXTRA_CONFIG_JSON`)
4. Hàm transform đặc thù của layer
5. `transform_data(df, ctx)`
6. `reconcile_job(df, ctx)` (gắn profile + chạy 5 DQ gates)
7. `main()` (đọc nguồn → transform → reconcile → `write_by_profile`)

### Yêu cầu tự code
1. **`notebooks/01_bronze/nb_brz_template.ipynb`**:
   - 1 notebook duy nhất chạy cho cả 7 bảng: đọc CSV từ `Files/landing/adventureworks/<batch_date>/<table_name>` qua `get_files_base_path()` → ghi `brz.<table_name>`.
2. **`notebooks/02_silver/nb_sil_generic.ipynb`**:
   - 1 notebook duy nhất chạy cho cả 7 bảng:
     - Đọc từ `brz.<table_name>`. Nếu có `WATERMARK_COLUMN` và `LAST_EXTRACT_DATE`, tính ngày bắt đầu = `LAST_EXTRACT_DATE + PRUNE_DAY` (lùi lại N ngày để bắt late-arriving data).
     - Gọi `dedup_source`, `run_all_dq_gates`, và `write_by_profile` → ghi `sil.<table_name>`.
3. **`notebooks/03_gold/dim/` & `notebooks/03_gold/fact/`**:
   - `dim/nb_gld_d_customer.ipynb`: Join 3 bảng Snowflake (`sil.customer` + `sil.customer_address` + `sil.address`), lọc địa chỉ chính, giữ lịch sử SCD2, union với `generate_type_safe_row_zero` → ghi `gld.gld_d_customer`.
   - `dim/nb_gld_d_product.ipynb`: Join `sil.product` + `F.broadcast(sil.product_category)`, union với Row 0 → ghi `gld.gld_d_product`.
   - `dim/nb_gld_d_date.ipynb`: Tự sinh bảng ngày (2000–2030) + Row 0 → ghi `gld.gld_d_date`.
   - `fact/nb_gld_f_sales.ipynb`: Join `sil.sales_order_header` + `sil.sales_order_detail`, **`LEFT JOIN`** sang `gld.gld_d_customer` (chỉ lấy `_is_current = true` hoặc join theo `OrderDate BETWEEN _valid_from AND _valid_to`) và `gld.gld_d_product`, dùng `COALESCE(dim.ROW_ID, 0)`, ghi vào `gld.gld_f_sales` theo `PARTITION_OVERWRITE` (`PRUNE_DAY = -20`).

### Bài Lab Kiểm Chứng Bắt Buộc (Lab 2 — Decoupled Fact/Dim & Spark UI)
1. **Chạy `day_1` full:** Kiểm tra đủ số dòng ở `brz`, `sil`, `gld`; `gld.gld_d_customer` và `gld.gld_d_product` đều có `ROW_ID = 0`.
2. **Test Decoupled Fact/Dim với `day_2`:**
   - **Bước A:** Chỉ chạy Bronze/Silver cho `SalesOrderHeader`, `SalesOrderDetail` và Gold `gld.gld_f_sales` (thuộc `BKD30MIN`). **KHÔNG chạy `gld_d_customer`** (`MANUAL_DIM`).
   - **Kiểm tra:** Tổng `SubTotal` / số lượng đơn ở `gld.gld_f_sales` phải **khớp 100%** với `sil.sales_order_header` của `day_2`. Khách hàng mới xuất hiện ở `day_2` phải đang mang `CUSTOMER_ROW_ID = 0`.
   - **Bước B:** Bây giờ chạy `MANUAL_DIM` (`Customer`, `CustomerAddress`, `Address` → `gld.gld_d_customer`), sau đó chạy lại `gld.gld_f_sales` (`PRUNE_DAY = -20`).
   - **Kiểm tra:** Những dòng Fact từng mang `CUSTOMER_ROW_ID = 0` ở Bước A đã **tự động cập nhật sang `ROW_ID` thật** của khách hàng mới!
3. **Soi Spark UI (`http://localhost:4040` / `18080`):**
   - Xác nhận `dim_product` dùng `BroadcastHashJoin` thay vì `SortMergeJoin`.
   - Xác nhận `fact_sales` khi `PARTITION_OVERWRITE` chỉ ghi đè đúng các partition có trong cửa sổ `PRUNE_DAY`, không quét/ghi lại toàn bảng.

---

## Phase 3: Control Plane & Metadata (`metadata/`, `src/etl_load_metadata.py`)

### Tư duy kiến trúc (Khi đã chạy tay xong → Gom cấu hình vào Control Plane)
Sau khi chạy tay 7 bảng ở Phase 2, bạn sẽ thấy việc nhớ tham số và watermark bằng tay không thể scale lên 1.000 bảng. Lúc này ta mới tạo **Control Plane** để lưu toàn bộ cấu hình tĩnh và trạng thái watermark tập trung, dùng **Selective MERGE** để cập nhật config mà không bao giờ xóa mất watermark.

### Yêu cầu tự code
1. **`metadata/etl_tables.sql`**:
   - Tạo 4 schemas: `etl`, `brz`, `sil`, `gld` (khớp 1-1 với `FabricDE`, lưu Delta qua Hive Metastore).
   - Tạo bảng `etl.etl_job` gồm các nhóm cột:
     - *Định danh & Định tuyến:* `JOB_ID`, `JOB_NAME`, `LAYER`, `BATCH_GROUP`, `RUNNER_TARGET`, `ACTIVE_FLG`.
     - *Nguồn & Đích:* `SRC_SCHEMA`, `SRC_TABLE`, `TGT_SCHEMA`, `TGT_TABLE`.
     - *Hành vi ETL:* `DEPENDENCY_JOB` (pipe-separated), `LOAD_TYPE`, `ENABLE_SOFT_DELETE`, `INTEGRATION_KEY`, `CHECK_DUP_KEY`, `WATERMARK_COLUMN`, `PRUNE_DAY` (bắt buộc `<= 0`), `PARTITION_COLUMN`, `EXTRA_CONFIG_JSON`.
     - *Runtime State (Nhóm E — tuyệt đối không bị ghi đè khi sync config):* `LAST_EXTRACT_DATE`, `LAST_WATERMARK_VALUE`, `LAST_RUN_STATUS`, `LAST_RUN_BATCH_ID`, `LAST_RUN_AT`.
   - Tạo bảng `etl.etl_job_run` (partition theo `run_date`): lưu `run_id`, `batch_id`, `job_name`, `status` (`SUCCEEDED | FAILED | SKIPPED_UPSTREAM_FAILED`), `started_at`, `ended_at`, `duration_sec`, `src_row_count`, `tgt_row_count`, `rejected_row_count`, `watermark_before`, `watermark_after`, `error_message`.

2. **`metadata/etl_jobs.json`**:
   - Khai báo đầy đủ jobs cho 7 bảng Bronze (`brz.*`), 7 bảng Silver (`sil.*`), và 4 bảng Gold (`gld.gld_d_customer`, `gld.gld_d_product`, `gld.gld_d_date`, `gld.gld_f_sales`).
   - **Lưu ý thiết kế DAG đa lịch:** `gld.gld_f_sales` chạy ở `BKD30MIN|DAILY` nên `DEPENDENCY_JOB` của nó **chỉ** trỏ tới `sil.sales_order_header|sil.sales_order_detail` (không trỏ vào `gld.gld_d_customer` đang chạy ở `DAILY|MANUAL_DIM`).

3. **`src/etl_load_metadata.py`**:
   - `validate_metadata_records(records)`: Chặn ngay trước khi đụng Spark nếu: trùng `JOB_NAME`, `LAYER`/`LOAD_TYPE` sai enum, `PRUNE_DAY > 0`, `LOAD_TYPE` là `SCD1`/`SCD2` mà thiếu `INTEGRATION_KEY`, `PARTITION_OVERWRITE` mà thiếu `PARTITION_COLUMN`.
   - `execute_selective_merge(spark, staged_df)`: Viết câu `MERGE INTO etl.etl_job` dùng toán tử null-safe `<=>` chỉ update các cột cấu hình tĩnh khi có thay đổi, **giữ nguyên** `LAST_EXTRACT_DATE` và `LAST_WATERMARK_VALUE`. Khi `NOT MATCHED BY SOURCE` trong phạm vi `(LAYER, BATCH_GROUP)` đang sync thì soft-deprecate (`ACTIVE_FLG = 'N'`), không xóa dòng của team/batch khác.

### Kiểm chứng (Lab 3 — Selective MERGE)
- Chạy sync `etl_jobs.json` lần 1 → kiểm tra đủ 18 jobs trong `etl.etl_job`.
- Tự `UPDATE` giả lập `LAST_WATERMARK_VALUE = '2026-01-15'` cho `sil.sales_order_header`, rồi sửa `PRUNE_DAY` trong JSON từ `-7` thành `-10` và chạy sync lại → kiểm tra `PRUNE_DAY` đã đổi nhưng `LAST_WATERMARK_VALUE` vẫn giữ nguyên `'2026-01-15'`.

---

## Phase 4: Ready-Queue DAG Orchestrator & Fail-Job Recovery (`src/etl_caller.py`)

### Tư duy kiến trúc
Khi 50 job chạy song song, không được cùng `INSERT` vào `etl.etl_job_run` (sẽ nổ `ConcurrentAppendException`). Đồng thời, khi 1 bảng Silver fail, các bảng Gold phụ thuộc phải tự động skip thay vì chạy bằng data cũ.

### Yêu cầu tự code
1. **`src/etl_caller.py`**:
   - `filter_jobs_by_batch_group(jobs, target_group)`: Tách token theo `|`, match chính xác.
   - `DAGReadyQueueEngine`:
     - Dựng đồ thị có hướng từ `DEPENDENCY_JOB` (báo `UnresolvedDependencyError` nếu thiếu cha hoặc cha bị `ACTIVE_FLG = 'N'`).
     - Kahn's topological sort → ném `CircularDependencyError` nếu có vòng lặp.
     - `get_downstream_impact(failed_job)` & `get_upstream_closure(target_jobs)`.
   - **Lock-free Logging:** Mỗi lần chạy 1 job, ghi 1 file JSON/Parquet riêng vào `Files/raw_log/<batch_id>/<job_name>.parquet`. Cuối batch, hàm `consolidate_logs(batch_id)` đọc toàn bộ `raw_log/<batch_id>/` và `APPEND` 1 lần duy nhất vào `etl.etl_job_run`, đồng thời cập nhật `LAST_WATERMARK_VALUE` / `LAST_RUN_STATUS` vào `etl.etl_job`.
   - **Local Runner (`scripts/run_pipeline_local.py`):** Nhận `--batch-group BKD30MIN` (hoặc `DAILY`, `MANUAL_DIM`), chạy các notebook bằng `papermill.execute_notebook(...)` theo thứ tự Ready-Queue (có thể dùng `ThreadPoolExecutor` chạy song song các node độc lập để test lock-free logging!).

### Bài Lab Kiểm Chứng Bắt Buộc (Lab 4 — Chaos Testing)
1. **Fail Cascade:** Cố tình ném lỗi ở `sil.sales_order_header` trong `day_3` → kiểm tra `etl.etl_job_run` ghi nhận `sil.sales_order_header` là `FAILED`, `gld.fact_sales` là `SKIPPED_UPSTREAM_FAILED`, còn `gld.dim_product` vẫn `SUCCEEDED`.
2. **Targeted Recovery:** Fix lỗi, chạy lại theo chế độ targeted (`--target-jobs gld.fact_sales`) dùng `get_upstream_closure` → chỉ chạy đúng nhánh bị lỗi, không chạy lại các nhánh đã xanh.

---

## Phase 5: Đưa Lên Microsoft Fabric

Khi đã thuần thục toàn bộ Phase 0–4 ở local:
1. Đặt biến môi trường `ENV=fabric` để [fabric_compat.py](file:///d:/Project/PrjDEFabric/config/fabric_compat.py) tự dùng `SparkSession.getActiveSession()` và đường dẫn `Files/`.
2. Chuyển 4 module trong `src/` sang Utility Notebooks (`%run`) hoặc giữ nguyên cấu trúc tùy chiến lược deployment.
3. Map `scripts/run_pipeline_local.py` sang **3 lớp Fabric Data Pipeline** như `FabricDE`:
   - `Master Pipeline` (Trigger theo lịch `BKD30MIN`, `DAILY`, `MANUAL`)
   - `Layer Pipeline` (`SRC2BRZ`, `BRZ2SIL`, `SIL2GLD`)
   - `Functional Pipeline` (`START_BATCH`, `RUN_NOTEBOOK_CALLER` dùng `notebookutils.notebook.runMultiple`, `END_BATCH`, `SEND_NOTIFICATION`).
4. Tinh chỉnh `concurrency` trong `runMultiple` theo gói Fabric Capacity (F2/F4/F8...) để tránh CU throttling và cấu hình Retry Policy cho các lỗi transient.
5. **CI/CD & Environment Promotion:** Dùng `parameter.yml` / `fabric-cicd` để tự động thay thế Workspace ID, Lakehouse ID khi deploy Dev → Test → Prod.

---

## Tổng Kết: Ma Trận Kiểm Chứng Các Case Production

Trước khi chuyển sang Microsoft Fabric, đảm bảo bạn đã tự tay test xanh toàn bộ các case sau:

| Nhóm | Case phải cover | Cách kiểm chứng |
| :--- | :--- | :--- |
| **Control Plane** | Selective MERGE | Sửa config tĩnh trong `etl_jobs.json`, sync lại → `LAST_WATERMARK_VALUE` không bị mất. |
| **Control Plane** | Partial Sync Isolation | Sync riêng nhóm `BKD30MIN` → không làm `ACTIVE_FLG = 'N'` các job của `DAILY` hay `MANUAL_DIM`. |
| **Data Ops** | Idempotency | Chạy lại `day_2` hai lần liên tiếp → số dòng Silver & Gold (kể cả SCD2) không đổi. |
| **Data Ops** | Duplicate Key | `day_2` có 2 dòng trùng `SalesOrderID` → `dedup_source` giữ đúng 1 dòng mới nhất theo `ModifiedDate`. |
| **Data Ops** | Late-Arriving & Prune | `day_2` có đơn hàng của `day_1` cập nhật trễ → `PRUNE_DAY = -7` bắt được và cập nhật đúng partition. |
| **Data Ops** | SCD2 + Out-of-Order | Đổi thuộc tính `Customer` ở `day_2` → dòng cũ `_is_current=false`, dòng mới `_is_current=true`; bản ghi đến trễ không đóng nhầm dòng mới hơn. |
| **Data Ops** | Soft Delete | `Product` bị gỡ khỏi snapshot `day_2` → `_is_deleted=true`, không mất dấu vết lịch sử. |
| **Data Quality** | Quarantine & Reconcile | Dòng `TotalDue < 0` bị tách ra quarantine; `src_row_count == tgt_row_count + rejected_row_count`. |
| **Gold Modeling** | Decoupled Fact/Dim | Chạy `fact_sales` trước `dim_customer` → đủ 100% doanh thu với `ROW_ID = 0`; chạy Dim sau → Fact tự vá `ROW_ID` thật. |
| **Orchestration** | Cycle & Fail Cascade | Tạo vòng lặp DAG → `CircularDependencyError`; làm cha fail → con `SKIPPED_UPSTREAM_FAILED`. |
| **Concurrency** | Lock-free `raw_log` | Chạy song song nhiều job cùng lúc → không nổ `ConcurrentAppendException` trên `etl.etl_job_run`. |
