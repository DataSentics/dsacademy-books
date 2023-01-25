# Databricks notebook source
# MAGIC %run ../initial_notebook

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "csv")
 .option("sep", ";")
 .option("encoding", "latin1")
 .option("header", True)
 .option("cloudFiles.schemaLocation", ratings_checkpoint_raw)
 .load(ratings_source)
 .writeStream
 .format("delta")
 .option("checkpointLocation", ratings_checkpoint_raw)
 .option("path", ratings_parsed_path)
 .option("delta.columnMapping.mode", "name")
 .option("mergeSchema", "true")
 .trigger(availableNow=True)
 .outputMode("append")
 .table("book_ratings_bronze"))
