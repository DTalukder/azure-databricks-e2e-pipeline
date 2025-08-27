# Databricks notebook source
# MAGIC %md
# MAGIC ## **Data Reading**

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window 

# COMMAND ----------

df = spark.read.format("parquet")\
        .load("abfss://bronze@dbendtoend.dfs.core.windows.net/orders")

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumnRenamed("_rescued_data", "rescued_data")

# COMMAND ----------

df = df.drop("rescued_data")

# COMMAND ----------

df = df.withColumn("order_date", F.to_timestamp("order_date", "yyyy-MM-dd"))  # adjust format
df.display()

# COMMAND ----------

df = df.withColumn("year", F.year(F.col("order_date")))
display(df)

# COMMAND ----------

df1 = df.withColumn("flag",F.dense_rank().over(Window.partitionBy("year").orderBy(F.desc("total_amount"))))
display(df1)

# COMMAND ----------

df1 = df1.withColumn("rank_flag",F.rank().over(Window.partitionBy("year").orderBy(F.desc("total_amount"))))
display(df1)

# COMMAND ----------

df1 = df1.withColumn("row_flag",F.row_number().over(Window.partitionBy("year").orderBy(F.desc("total_amount"))))
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Classes - OOP**

# COMMAND ----------

class windows:

    def dense_rank(self,df):

        df_dense_rank = df.withColumn("flag",F.dense_rank().over(Window.partitionBy("year").orderBy(F.desc("total_amount"))))

        return df_dense_rank
    
    def rank(self,df):

        df_rank = df.withColumn("flag",F.dense_rank().over(Window.partitionBy("year").orderBy(F.desc("total_amount"))))

        return df_rank
    
    def row_numberk(self,df):

        row_number = df.withColumn("flag",F.dense_rank().over(Window.partitionBy("year").orderBy(F.desc("total_amount"))))

        return row_number
        

# COMMAND ----------

df_new = df

# COMMAND ----------

df_new.display()

# COMMAND ----------

obj = windows()

# COMMAND ----------

df_result = obj.dense_rank(df_new)
df_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@dbendtoend.dfs.core.windows.net/orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cat.silver.orders_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@dbendtoend.dfs.core.windows.net/orders'