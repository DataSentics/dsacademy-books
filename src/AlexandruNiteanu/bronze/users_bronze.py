# Databricks notebook source
# notebook for reading the data from the storage and write it as parquet
# used for BX-Users

# COMMAND ----------

# MAGIC %run ../autoloader

# COMMAND ----------

# MAGIC %run ../paths_database

# COMMAND ----------

df_users = autoload(
    users_path_raw,
    "csv",
    users_raw_checkpoint,
    delimiter=";",
)

# COMMAND ----------

df_users.writeStream.format("delta").option(
    "checkpointLocation",
    users_raw_checkpoint,
).option("path", users_path_parsed).trigger(availableNow = True).outputMode(
    "append"
).table(
    "bronze_users"
)
