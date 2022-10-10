# Databricks notebook source
# notebook for reading the data from the storage and write it as parquet
# used for users-pii

# COMMAND ----------

# MAGIC %run ../autoloader

# COMMAND ----------

# MAGIC %run ../paths_database

# COMMAND ----------

df_pii_users = autoload(
    pii_path_raw,
    "json",
    pii_users_raw_checkpoint,
    delimiter=','
)

# COMMAND ----------

df_pii_users.writeStream.format("parquet").option(
    "checkpointLocation",
    pii_users_raw_checkpoint,
).option("path", pii_path_parsed).outputMode(
    "append"
).table(
    "bronze_users_pii"
)
