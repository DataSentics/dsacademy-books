# Databricks notebook source
# MAGIC %run ../initial_notebook

# COMMAND ----------

autoload_to_table(users_pii_source, "users_pii_bronze", users_pii_checkpoint_raw,
                  users_pii_parsed_path, source_format='json')
(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "json")
.option("encoding", "latin1")
.option("header", True)
.option("cloudFiles.schemaLocation", users_pii_checkpoint_raw)
.load(users_pii_source)
.writeStream
.format("delta")
.option("checkpointLocation", users_pii_checkpoint_raw)
.option("path", users_pii_parsed_path)
.option("delta.columnMapping.mode", "name")
.option("mergeSchema", "true")
.trigger(availableNow=True)
.outputMode("append")
.table("users_pii_bronze"))
