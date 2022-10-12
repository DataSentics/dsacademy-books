# Databricks notebook source
from pyspark.sql.functions import col
import time

# COMMAND ----------

# MAGIC %run ../includes/includes_silver

# COMMAND ----------

# Ingest & clean data from bronze ratings
df_rating = (
    spark
    .readStream
    .table("bronze_ratings")
    .withColumn("Book-Rating", col("Book-Rating").cast("Integer"))
)

# COMMAND ----------

dbutils.fs.rm(checkpoint_ratings_path, True)

# COMMAND ----------

(
    df_rating
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_ratings_path)
    .option("path", ratings_output_path)
    .option("mergeSchema", "true")
    .trigger(once=True)
    .outputMode("append")
    .table("silver_ratings")
)
