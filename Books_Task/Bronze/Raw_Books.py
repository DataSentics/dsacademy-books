# Databricks notebook source
# MAGIC %run ../init_notebook

# COMMAND ----------

import booksutilities.bookslibrary as b

# COMMAND ----------

(spark
 .readStream
 .format("cloudFiles")
 .option("cloudFiles.format", 'csv')
 .option("sep", ';')
 .option("encoding", "latin1")
 .option("header", True)
 .option("cloudFiles.schemaLocation", b.books_checkpoint_raw)
 .load(b.books_path)
 .writeStream
 .format("delta")
 .option("checkpointLocation", b.books_checkpoint_raw)
 .option("mergeSchema", "true")
 .option("path", b.books_bronze_path)
 .trigger(availableNow=True)
 .outputMode("append")
 .table('books_bronze'))

# COMMAND ----------

display(spark.table('books_bronze'))
