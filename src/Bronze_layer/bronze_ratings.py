# Databricks notebook source
# MAGIC %run ../Autoloader

# COMMAND ----------

# MAGIC %run ../Set_paths/bronze_paths

# COMMAND ----------

data_loader = autoloader(
    rating_path,
    "csv",
    ratings_checkpoint,
    ";",
)

# COMMAND ----------

data_loader.writeStream.format("delta").option(
    "checkpointLocation",
    ratings_checkpoint,
).option("path", ratings_output_path).trigger(availableNow=True).outputMode("append").table("bronze_ratings")
