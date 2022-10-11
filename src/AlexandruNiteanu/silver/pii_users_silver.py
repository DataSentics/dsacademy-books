# Databricks notebook source
# notebook for cleaning the data
# used for pii_users file

# COMMAND ----------

# MAGIC %run ../paths_database

# COMMAND ----------

df_pii_users = spark.readStream.table("bronze_users_pii")

# COMMAND ----------

df_pii_users.writeStream.format("delta").option(
    "checkpointLocation", pii_users_checkpoint
).option("path", pii_path_cleansed).trigger(availableNow=True).outputMode("append").table("silver_users_pii")
