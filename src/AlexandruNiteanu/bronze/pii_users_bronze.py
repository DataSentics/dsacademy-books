# Databricks notebook source
# notebook for reading the data from the storage and write it as parquet
# used for users-pii

# COMMAND ----------

# MAGIC %run ../autoloader

# COMMAND ----------

# MAGIC %run ../paths_database

# COMMAND ----------

df_pii_users = autoload(
    f"{storage}".format("01rawdata")
    + "books_crossing/",
    "json",
    f"{dbx_file_system}piiUsers_raw_checkpoint/",
    delimiter=','
)

# COMMAND ----------

df_pii_users.writeStream.format("delta").option(
    "checkpointLocation",
    f"{dbx_file_system}piiUsers_raw_checkpoint/",
).option("path", f"{storage}".format("02parseddata")
    + "AN_Books/users_pii").trigger(availableNow=True).outputMode(
    "append"
).table(
    "bronze_users_pii"
)
