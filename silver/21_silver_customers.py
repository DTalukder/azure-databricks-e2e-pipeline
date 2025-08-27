# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import functions as T

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Data Reading**

# COMMAND ----------

df = spark.read.format("parquet")\
    .load("abfss://bronze@dbendtoend.dfs.core.windows.net/customers")

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

df = df.withColumn("domains", F.split(F.col("email"), "@")[1])
df.display()

# COMMAND ----------

df.groupBy("domains").agg(F.count("customer_id").alias("total_customers")).sort("total_customers",ascending=False).display() 

# COMMAND ----------

# df_gmail = df.filter(F.col("domains")=="gmail.com")
# df_gmail.display()

# df_yahoo = df.filter(F.col("domains")=="yahoo.com")
# df_yahoo.display()

# df_hotmail = df.filter(F.col("domains")=="hotmail.com")
# df_hotmail.display()

# COMMAND ----------

df = df.withColumn("full_name", F.concat(F.col("first_name"),F.lit(" "),F.col("last_name")))
df = df.drop("first_name","last_name")

display(df)

# COMMAND ----------

df.write.mode("overwrite").format("delta").save("abfss://silver@dbendtoend.dfs.core.windows.net/customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cat.silver.customers_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@dbendtoend.dfs.core.windows.net/customers'