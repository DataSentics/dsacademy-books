# Databricks notebook source
# notebook for cleaning the data
# used for pii_users file

# COMMAND ----------

# MAGIC %run ../paths_database

# COMMAND ----------

df_pii_users = spark.readStream.table("bronze_users_pii")

# COMMAND ----------

df_pii_users.writeStream.format("delta").option(
    "checkpointLocation", f"{dbx_file_system}silver_piiusers_checkpoint/"
).option("path", f"{storage}".format("03cleanseddata")
    + "AN_Books/pii_users_silver").trigger(availableNow=True).outputMode("append").table("silver_users_pii")
