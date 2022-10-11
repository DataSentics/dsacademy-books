# Databricks notebook source
# MAGIC %run ../setup/includes_silver

# COMMAND ----------

# the pii json is clean so it does not need anymore cleaning
df_pii = spark.readStream.table("bronze_pii")

# COMMAND ----------

(
    df_pii
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_userspii_path)
    .option("path", pii_output_path)
    .trigger(availableNow=True)
    .outputMode("append")
    .table("silver_pii")
)
