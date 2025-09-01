# Lessons Learned

- Quotas first; UC-first naming; DLT runs via Pipelines.
- Autoloader: `readStream` + `schemaLocation` + `.trigger(once=True)`.
- PySpark: `functions as F` + consistent `F.*`.
- Dim/Fact: enforce FKs; choose surrogate-key approach.
- Safe to read UC tables during runs.
- Branch-first git; protect `main`.
- Most friction came from environment assumption mismatches.
