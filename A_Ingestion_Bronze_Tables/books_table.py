# Databricks notebook source
# MAGIC %run ../initial_notebook

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "csv")
 .option("sep", ";")
 .option("encoding", "latin1")
 .option("header", True)
 .option("cloudFiles.schemaLocation", books_checkpoint_raw)
 .load(books_source)
 .writeStream
 .format("delta")
 .option("checkpointLocation", books_checkpoint_raw)
 .option("path", books_parsed_path)
 .option("delta.columnMapping.mode", "name")
 .option("mergeSchema", "true")
 .trigger(availableNow=True)
 .outputMode("append")
 .table("books_bronze"))
