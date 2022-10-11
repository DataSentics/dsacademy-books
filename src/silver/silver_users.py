# Databricks notebook source
from pyspark.sql.functions import col, split, when
import time

# COMMAND ----------

# MAGIC %run ../includes/includes_silver

# COMMAND ----------

# Ingest & clean data from bronze users
users_df = (
    spark.readStream.table("bronze_users")
    .withColumn("city", split(col("location"), ",").getItem(0))
    .withColumn("state", split(col("location"), ",").getItem(1))
    .withColumn("country", split(col("location"), ",").getItem(2))
    .withColumn("Age", when(col("Age") == "NULL", "unknown").otherwise(col("Age")).cast("Integer"))
    .withColumn("city", when(col("city") == "n/a", "unknown").otherwise(col("city")))
    .withColumn("state", when(col("state") == "n/a", "unknown").otherwise(col("state")))
    .fillna("unknown")
    .drop("location")
)

# COMMAND ----------

(
    users_df
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_users_path)
    .option("path", users_output_path)
    .outputMode("append")
    .table("silver_users")
)

# COMMAND ----------

time.sleep(10)

# COMMAND ----------

dbutils.fs.rm(checkpoint_users_path, True)

# COMMAND ----------


