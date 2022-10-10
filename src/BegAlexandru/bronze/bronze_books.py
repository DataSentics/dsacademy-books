# Databricks notebook source
import time

# COMMAND ----------

# MAGIC %run ../AutoLoader

# COMMAND ----------

# MAGIC %run ../setup/includes_bronze

# COMMAND ----------

Loading_data = auto_loader(
    books_path,
    "csv",
    checkpoint_books_path,
    ";",
)

# COMMAND ----------

(
    Loading_data
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_write_books_path)
    .option("path", books_output_path)
    .outputMode("append")
    .table("bronze_books")
)

# COMMAND ----------

time.sleep(10)

# COMMAND ----------

dbutils.fs.rm(checkpoint_books_path, True)
dbutils.fs.rm(checkpoint_write_books_path, True)
