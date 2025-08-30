# Master Pain Points (Azure Databricks E2E)

## 1) Clusters & Quotas
- Tried to run DLT on Spark Connect → always fails; DLT must run via **Pipelines UI/Jobs**.
- Azure vCPU quotas limited cluster sizes (10 → 20 vCPUs after request).
- Overprovisioned a 36-core cluster for MB-scale data.
- Photon toggle confusion; learned **Photon ON** for SQL/Delta.
- Debated single-node vs job clusters; small job clusters (1–2 workers) are fine for dev.

## 2) Pipeline vs Notebook Execution
- Tutor ran interactively; your context differed (classic/HMS vs UC).
- DLT code is **not** “run cell-by-cell”; notebooks are sources, execution is in **Pipelines**.

## 3) Storage & Catalog Confusion
- UC vs HMS mismatch → always fully qualify `catalog.schema.table`.
- Schema naming uncertainty; bronze/source mix-ups; accidental raw delete fear.

## 4) Streaming vs Batch
- Used `spark.read` with Autoloader → fails; must use `readStream`.
- Missing `cloudFiles.schemaLocation` unless cells ran in order.
- Needed `.trigger(once=True)` to run streaming like batch.
- “WAITING_FOR_RESOURCES” tied to quotas/bad refs.

## 5) PySpark Function Errors
- Missing `from pyspark.sql import functions as F` → `year` undefined, `dense_rank` confusion.
- `split`/`concat` mistakes; rename order led to unresolved columns.

## 6) Gold Layer Challenges
- NULL FKs in fact (normal when dims don’t match).
- Unsure on surrogate keys (`monotonically_increasing_id` vs `row_number`).
- Join/rename confusion (`df.col` vs `df['col']`, `old_customer_id`).

## 7) Orchestration/DAG Friction
- Needed retries on Gold steps.
- First DLT run ~12 min (spin-up + expectations); worried about querying during runs (it’s safe).

## 8) Tutorial Quality & Style
- Course mismatch; skipped imports/setup; code looked “magical.”
- Led to 6 days of debugging and energy drain.

## 9) Workflow Inefficiencies
- Notebook duplicates; intermittent display() behavior (caching/catalog).
- Cost anxiety about “Run All” (cost = cluster uptime, not cells).

## 10) Productivity & Motivation
- “Last mile” stalls, delay spirals, frustration loops.

## 11) Career Confidence
- SQL self-doubt; imposter concerns; unsure which skills to spotlight.
