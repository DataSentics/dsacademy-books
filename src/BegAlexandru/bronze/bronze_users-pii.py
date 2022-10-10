# Databricks notebook source
import time

# COMMAND ----------

# MAGIC %run ../AutoLoader

# COMMAND ----------

# MAGIC %run ../setup/includes_bronze

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
    .option("checkpointLocation", checkpoint_write_users-pii_path)
    .option("path", users_pii_output_path)
    .outputMode("append")
    .table("bronze_pii")
)

# COMMAND ----------

time.sleep(10)

# COMMAND ----------

dbutils.fs.rm(checkpoint_users_pii_path, True)
dbutils.fs.rm(checkpoint_write_users-pii_path, True)
