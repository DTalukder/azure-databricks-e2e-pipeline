# Pain Points

**Cluster & quotas** — Tried to run DLT on Spark Connect, hit a 10-vCPU quota, over-provisioned once, and went back and forth on Photon/single-node — lost time.

**Streaming vs batch** — Used `read` instead of `readStream`, missed `cloudFiles.schemaLocation`, and misunderstood `.trigger(once=True)` — led to failures/waits.

**PySpark functions** — API misuse (`F.year`, `F.dense_rank`, `split`/`concat`) caused NameErrors and column resolution errors.

**Orchestration/DAG** — Stabilized the DAG; initial runs needed retries and the first DLT warm-up (cluster spin-up + expectations).
