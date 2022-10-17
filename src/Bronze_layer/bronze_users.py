# Databricks notebook source
# MAGIC %run ../Autoloader

# COMMAND ----------

# MAGIC %run ../Set_paths/bronze_paths

# COMMAND ----------

data_loader = autoloader(
    users_path,
    "csv",
    users_checkpoint,
    ";",
)

# COMMAND ----------

data_loader.writeStream.format("delta").option(
    "checkpointLocation",
    users_write_path,
).option("path", users_output_path).trigger(availableNow=True).outputMode("append").table("bronze_users")
