
End-to-end pipeline on **Azure Databricks (Unity Catalog)** using **Delta Lake**, **Delta Live Tables**, **Autoloader**, and **PySpark**.

- `config/00_config.py`
- `bronze/10_bronze_autoload.py`
- `silver/20_silver_orders.py`, `21_silver_customers.py`, `22_silver_products.py`, `23_silver_regions.py`
- `gold/30_gold_dim_customers.py`, `31_gold_dim_products_dlt.py` (DLT via Pipelines), `32_gold_fact_orders.py`
- `smoke/00_smoke_test.py` (temporary; upgraded on Day 3)
