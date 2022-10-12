# Databricks notebook source
# MAGIC %run ../includes/includes_bronze

# COMMAND ----------

# MAGIC %run ../Autoloader

# COMMAND ----------

Loading_users = auto_loader(
    users_path,
    "csv",
    checkpoint_users_path,
    ";",
)

# COMMAND ----------

(
    Loading_users
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_write_users_path)
    .option("path", users_output_path)
    .option("overwriteSchema", "true")
    .trigger(availableNow=True)
    .outputMode("append")
    .table("bronze_users")
)
