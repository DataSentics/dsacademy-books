# Databricks notebook source
# notebook for reading the data from the storage and write it as parquet
# used for BX-Users

# COMMAND ----------

# MAGIC %run ../autoloader

# COMMAND ----------

# MAGIC %run ../paths_database

# COMMAND ----------

df_users = autoload(
    f"{storage}Bx-Users/".format("alexandruniteanu"),
    "csv",
    f"{dbx_file_system}raw_users_checkpoint/",
    delimiter=";",
)

# COMMAND ----------

df_users.writeStream.format("delta").option(
    "checkpointLocation",
    f"{dbx_file_system}raw_users_checkpoint/",
).option("path", f"{storage}".format("02parseddata")
    + "AN_Books/users").trigger(availableNow=True).outputMode(
    "append"
).table(
    "bronze_users"
)
