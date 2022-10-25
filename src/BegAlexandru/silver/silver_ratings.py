# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# run Write Function using df, checkpoint, output_path, table_name

# COMMAND ----------

# MAGIC %run ../auto_loader_and_stream_writer

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

write_stream_azure_append(
    df_rating,
    checkpoint_ratings_path,
    rating_output_path,
    "silver_ratings"
)
