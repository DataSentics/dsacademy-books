# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import LongType

# COMMAND ----------

# MAGIC %run ../includes/includes_silver

# COMMAND ----------

# Read users pii
df_pii = spark.readStream.table("bronze_pii").withColumn("User-ID", col("User-ID").cast(LongType()))

# COMMAND ----------

(
    df_pii
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_pii_path)
    .option("path", pii_output_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .outputMode("append")
    .table("silver_pii")
)
