# Databricks notebook source
# MAGIC %run ../initial_notebook

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("sep", ";")
.option("encoding", "latin1")
.option("header", True)
.option("cloudFiles.schemaLocation", users_checkpoint_raw)
.load(users_source)
.writeStream
.format("delta")
.option("checkpointLocation", users_checkpoint_raw)
.option("path", users_parsed_path)
.option("delta.columnMapping.mode", "name")
.option("mergeSchema", "true")
.trigger(availableNow=True)
.outputMode("append")
.table("users_bronze"))
