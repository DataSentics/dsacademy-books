# Databricks notebook source
from pyspark.sql.functions import col
import time

# COMMAND ----------

# MAGIC %run ../setup/includes_silver

# COMMAND ----------

# cleaning the data from bronze rating

# COMMAND ----------

df_rating = (
    spark
    .readStream
    .table("bronze_ratings")
    .withColumn("Book-Rating", col("Book-Rating").cast("Integer"))
)

# COMMAND ----------

(
    df_rating
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_ratings_path)
    .option("path", rating_output_path)
    .outputMode("append")
    .table("silver_ratings")
)

# COMMAND ----------

time.sleep(10)

# COMMAND ----------

dbutils.fs.rm(checkpoint_ratings_path, True)
