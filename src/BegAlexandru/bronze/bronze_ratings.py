# Databricks notebook source
import time

# COMMAND ----------

# MAGIC %run ../AutoLoader

# COMMAND ----------

# MAGIC %run ../setup/includes_bronze

# COMMAND ----------

Loading_ratings = auto_loader(
    ratings_path,
    "csv",
    checkpoint_ratings_path,
    ";",
)

# COMMAND ----------

(
    Loading_ratings
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_write_ratings_path)
    .option("path", books_rating_output_path)
    .outputMode("append")
    .table("bronze_ratings")
)

# COMMAND ----------

time.sleep(10)

# COMMAND ----------

dbutils.fs.rm(checkpoint_ratings_path, True)
dbutils.fs.rm(checkpoint_write_ratings_path, True)
