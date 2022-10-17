# Databricks notebook source
# MAGIC %run ../Autoloader

# COMMAND ----------

# MAGIC %run ../Set_paths/bronze_paths

# COMMAND ----------

data_loader = autoloader(
    books_path,
    "csv",
    books_checkpoint,
    ";",
)

# COMMAND ----------

data_loader.writeStream.format("delta").option(
    "checkpointLocation",
    books_checkpoint,
).option("path", books_output_path).trigger(availableNow=True).outputMode("append").table("bronze_books")
