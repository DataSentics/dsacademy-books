# Databricks notebook source
# MAGIC %run ../setup/initial_book_setup

# COMMAND ----------

import pipelineutils.paths as P

# COMMAND ----------

(spark
 .readStream
 .format("cloudFiles")
 .option("sep", ";")
 .option("encoding", "latin1")
 .option("cloudFiles.format", "csv")
 .option("cloudFiles.schemaLocation", P.bronze_users_checkpoint_path)
 .load(P.users_path)
 .writeStream
 .format("delta")
 .option("checkpointLocation", P.bronze_users_checkpoint_path)
 .option("mergeSchema", "true")
 .option("path", P.bronze_users_path)
 .trigger(availableNow=True)
 .outputMode("append")
 .table("users_bronze")
 )
