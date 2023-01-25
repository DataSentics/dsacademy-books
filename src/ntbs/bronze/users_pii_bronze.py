# Databricks notebook source
# MAGIC %run ../setup/initial_book_setup

# COMMAND ----------

import pipelineutils.paths as P

# COMMAND ----------

(spark
 .readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 .option("cloudFiles.schemaLocation", P.bronze_users_pii_checkpoint_path)
 .load(P.users_pii_path)
 .writeStream
 .format("delta")
 .option("checkpointLocation", P.bronze_users_pii_checkpoint_path)
 .option("mergeSchema", "true")
 .option("path", P.bronze_users_pii_path)
 .trigger(availableNow=True)
 .outputMode("append")
 .table("users_pii_bronze")
 )
