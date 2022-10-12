# Databricks notebook source
# MAGIC %run ../Autoloader

# COMMAND ----------

# MAGIC %run ../Set_paths/bronze_paths

# COMMAND ----------

data_loader = autoloader(
    users_pii_path,
    "json",
    users_pii_checkpoint,
    ",",
)

# COMMAND ----------

data_loader.writeStream.format("delta").option(
    "checkpointLocation",
    users_pii_checkpoint,
).option("path", users_pii_output_path).trigger(availableNow=True).outputMode("append").table("bronze_users_pii")
