# Databricks notebook source
# MAGIC %md
# MAGIC # **FACT ORDERS**

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Reading**

# COMMAND ----------

df = spark.sql("select * from databricks_cat.silver.orders_silver")

# COMMAND ----------

df_dimcus = spark.sql("select DimCustomerKey, customer_id as dim_customer_id from databricks_cat.gold.dimcustomers")

df_dimpro = spark.sql("select product_id as DimProductkKey, product_id as dim_product_id from databricks_cat.gold.dimproducts")

# COMMAND ----------

# MAGIC %md
# MAGIC **Fact Dataframe**

# COMMAND ----------

df_fact = df.join(df_dimcus, df['customer_id'] == df_dimcus['dim_customer_id'],how='left').join(df_dimpro, df['product_id'] == df_dimpro['dim_product_id'],how='left')

df_fact_new = df_fact.drop('dim_customer_id','dim_product_id', 'customer_id', 'product_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Upsert on Fact Table**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("databricks_cat.gold.FactOrders"):
    pass
    dlt_obj = DeltaTable.forName(spark, "databricks_cat.gold.FactOrders")

    dlt_obj.alias("trg").merge(df_fact_new.alias("src"), "trg.order_id = src.order_id AND trg.DimCustomerKey = src.DimCustomerKey AND trg.DimProductkKey = src.DimProductkKey")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

else:
    df_fact_new.write.format("delta")\
        .option("path", "abfss://gold@dbendtoend.dfs.core.windows.net/FactOrders")\
        .saveAsTable("databricks_cat.gold.FactOrders")