# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window

# COMMAND ----------

#init_load_flag = int(dbutils.widgets.get("init_load_flag"))

init_load_flag = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading From Source**

# COMMAND ----------

df = spark.sql("select * from databricks_cat.silver.customers_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Removing Duplicates**

# COMMAND ----------

df = df.dropDuplicates(subset=['customer_id'])


# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Dividing New vs Old Recrods**

# COMMAND ----------

if init_load_flag == 0:

    df_old = spark.sql('''select DimCustomerKey, customer_id, create_date, update_date
                       from databricks_cat.gold.DimCustomers''')

    
else:
    df_old = spark.sql('''select 0 DimCustomerKey, 0 customer_id, 0 create_date, 0 update_date
                       from databricks_cat.silver.customers_silver
                       where 1=0''')

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Renaming Columns of df_old**

# COMMAND ----------

df_old = (
    df_old
      .withColumnRenamed('DimCustomerKey', 'old_DimCustomerKey')
      .withColumnRenamed('Customer_id',     'old_customer_id') 
      .withColumnRenamed('Create_date',     'old_create_date')
      .withColumnRenamed('Update_date',     'old_update_date')
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Applying Join with Old records**

# COMMAND ----------

df_join = df.join(df_old, df['customer_id'] == df_old['old_customer_id'], 'left')

# COMMAND ----------

df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Separating New vs Old Records**

# COMMAND ----------

df_new = df_join.filter(df_join['old_DimCustomerKey'].isNull())

# COMMAND ----------

df_old = df_join.filter(df_join['old_DimCustomerKey'].isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing df_old**

# COMMAND ----------

# Dropping all the columns which are not required

df_old = df_old.drop('old_customer_id', 'old_update_date')

# Reanaming "old_DimCustomerKey" column to "DimCustomerKey"

df_old = df_old.withColumnRenamed('old_DimCustomerKey', 'DimCustomerKey')

# Renaming 'old_create_date' column to 'create_date'

df_old = df_old.withColumnRenamed('old_create_date', 'create_date')
df_old = df_old.withColumn('create_date', F.to_timestamp(F.col('create_date')))

#Recreating "update_date" column with current timestamp

df_old = df_old.withColumn('update_date', F.current_timestamp())

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing df_new**

# COMMAND ----------

# Dropping all the columns which are not required

df_new = df_new.drop('old_DimCustomerKey', 'old_customer_id','old_update_date', 'old_create_date')

# Recreating "update_date", "current_date" columns with current timestamp

df_new = df_new.withColumn('update_date', F.current_timestamp())
df_new = df_new.withColumn('create_date', F.current_timestamp())

# COMMAND ----------

df_new.display()

# COMMAND ----------

# Get current max key (0 if table doesn’t exist/empty)
if spark.catalog.tableExists("databricks_cat.gold.DimCustomers"):
    max_key = spark.table("databricks_cat.gold.DimCustomers") \
                   .agg(F.max("DimCustomerKey")).first()[0] or 0
else:
    max_key = 0

# Define window with partitioning
w = Window.partitionBy(F.lit(1)).orderBy("customer_id")

# Apply transformations
df_new = (df_new
    # Add surrogate key with offset
    .withColumn("DimCustomerKey", F.row_number().over(w) + F.lit(int(max_key)))

    # Drop unnecessary columns
    .drop('old_DimCustomersKey', 'old_customer_id', 'old_update_date', 'old_create_date')

    # Add timestamps
    .withColumn('update_date', F.current_timestamp())
    .withColumn('create_date', F.current_timestamp())

    # Reorder columns
    .select(
        'customer_id', 'email', 'city', 'state', 'domains',
        'full_name', 'DimCustomerKey', 'update_date', 'create_date'
    )
)


# COMMAND ----------

# this works but not as robust.
# Dropping all the columns which are not required

# df_new = df_new.drop('old_DimCustomersKey', 'old_customer_id', 'old_update_date', 'old_create_date')


#Recreating "update_date", "current_date" columns with current timestamp

# df_new = df_new.withColumn('update_date', F.current_timestamp())
# df_new = df_new.withColumn('current_date', F.current_timestamp())

### THIS:
# Add surrogate key here
# df_new = df_new.withColumn("DimCustomerKey", F.monotonically_increasing_id() + F.lit(1))

### OR THIS:
# Create surrogate key using row_number
# from pyspark.sql.window import Window

# window_spec = Window.orderBy("customer_id")
# df_new = df_new.withColumn("DimCustomerKey", F.row_number().over(window_spec))


# Reorder the columns
# df_new = df_new.select(
#     'customer_id', 'email', 'city', 'state', 'domains',
#     'full_name', 'DimCustomerKey', 'update_date', 'current_date'
# )


# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Surrogate Keys - from 1**

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", F.monotonically_increasing_id() + F.lit(1))

# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Adding Max Surrogate Key**

# COMMAND ----------

if init_load_flag == 1:
  max_surrogate_key = 0
else:
  df_maxsur = spark.sql("select max(DimCustomerKey) as max_surrogate_key from databricks_cat.gold.DimCustomers")
  
  #Converting df_maxsur to max_surrogate_key 
  max_surrogate_key = df_maxsur.collect()[0]['max_surrogate_key']

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", F.lit(max_surrogate_key) + F.col("DimCustomerKey"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Union of df_old and df_new**

# COMMAND ----------

# Ensure df_old has DimCustomerKey and matches df_new schema
df_old = df_old.withColumn("DimCustomerKey", F.lit(None).cast(T.LongType()))

# Reorder columns to match df_new
df_old = df_old.select(*df_new.columns)

# COMMAND ----------

df_final = df_new.unionByName(df_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **SCD Type - 1**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("databricks_cat.gold.DimCustomers"):
  dlt_object = DeltaTable.forPath(spark, "abfss://gold@dbendtoend.dfs.core.windows.net/DimCustomers")

  dlt_object.alias("trg").merge(df_final.alias("src"), "trg.DimCustomerKey = src.DimCustomerKey")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()

else:
 
  df_final.write.mode("overwrite")\
  .format("delta")\
  .option("path", "abfss://gold@dbendtoend.dfs.core.windows.net/DimCustomers")\
  .saveAsTable("databricks_cat.gold.DimCustomers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from databricks_cat.gold.dimcustomers