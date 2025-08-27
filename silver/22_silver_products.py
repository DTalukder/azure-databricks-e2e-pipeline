# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import functions as T

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Data Reading**

# COMMAND ----------

df = spark.read.format("parquet")\
    .load("abfss://bronze@dbendtoend.dfs.core.windows.net/products")

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Functions**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP FUNCTION discount_func(p_price DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC RETURN p_price * 0.90;

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, price, discount_func(price) as discounted_price 
# MAGIC from products

# COMMAND ----------

df = df.withColumn("discounted_price",F.expr("discount_func(price)"))

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP FUNCTION upper_func(p_brand STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC AS $$
# MAGIC   return p_brand.upper()
# MAGIC $$;
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select product_id, brand, upper_func(brand) as brand_upper
# MAGIC from products
# MAGIC
# MAGIC -- use only if a complex function is needed

# COMMAND ----------

df.write.mode("delta")\
    .mode("overwrite")\
    .option("path","abfss://silver@dbendtoend.dfs.core.windows.net/products")\
    .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cat.silver.products_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@dbendtoend.dfs.core.windows.net/products'