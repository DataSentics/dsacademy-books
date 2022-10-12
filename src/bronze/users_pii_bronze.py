# Databricks notebook source
# MAGIC %run ../includes/includes_bronze

# COMMAND ----------

# MAGIC %run ../Autoloader

# COMMAND ----------

Loading_userspii = auto_loader(
    users_pii_path,
    "json",
    checkpoint_users_pii_path,
    ",",
)

# COMMAND ----------

(
    Loading_userspii
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_write_users_pii_path)
    .option("path", users_pii_output_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .outputMode("append")
    .table("bronze_pii")
)
