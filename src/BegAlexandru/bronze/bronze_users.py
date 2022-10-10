# Databricks notebook source
import time

# COMMAND ----------

# MAGIC %run ../AutoLoader

# COMMAND ----------

# MAGIC %run ../setup/includes_bronze

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
    .outputMode("append")
    .table("bronze_users")
)

# COMMAND ----------

time.sleep(10)

# COMMAND ----------

dbutils.fs.rm(checkpoint_users_path, True)
dbutils.fs.rm(checkpoint_write_users_path, True)
