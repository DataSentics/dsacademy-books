# Databricks notebook source
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
    .trigger(availableNow=True)
    .outputMode("append")
    .table("bronze_books")
)
