# Databricks notebook source
# MAGIC %run ../includes/includes_bronze

# COMMAND ----------

# MAGIC %run ../Autoloader

# COMMAND ----------

ingested_data = auto_loader(
    books_path,
    "csv",
    checkpoint_books_path,
    ";",
)

# COMMAND ----------

(
    ingested_data
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_write_books_path)
    .option("path", books_output_path)
    .option("overwriteSchema", "true")
    .trigger(availableNow=True)
    .outputMode("append")
    .table("bronze_books")
)
