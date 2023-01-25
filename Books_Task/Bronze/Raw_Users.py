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
 .option("cloudFiles.schemaLocation", b.users_checkpoint_raw)
 .load(b.users_path)
 .writeStream
 .format("delta")
 .option("checkpointLocation", b.users_checkpoint_raw)
 .option("mergeSchema", "true")
 .option("path", b.users_bronze_path)
 .trigger(availableNow=True)
 .outputMode("append")
 .table('users_bronze'))

# COMMAND ----------

display(spark.table('users_bronze'))
