# Databricks notebook source
# MAGIC %run /Repos/Book_Task/dsacademy-books/utilities/db_notebook

# COMMAND ----------

import utilities.utilities as u

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", 'csv')
 .option("sep", ';')
 .option("encoding", "latin1")
 .option("header", True)
 .option("cloudFiles.schemaLocation", u.ratings_checkpoint_bronze)
 .load(u.ratings_path)
 .writeStream
 .format("delta")
 .option("checkpointLocation", u.ratings_checkpoint_bronze)
 .option("mergeSchema", "true")
 .option("path", u.ratings_bronze_path)
 .trigger(availableNow=True)
 .outputMode("append")
 .table('ratings_bronze'))
