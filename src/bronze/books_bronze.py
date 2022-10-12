# Databricks notebook source
# MAGIC %run ../includes/includes_bronze

# COMMAND ----------

# MAGIC %run ../Autoloader

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
    .option("overwriteSchema", "true")
    .trigger(availableNow=True)
    .outputMode("append")
    .table("bronze_books")
)
