# Databricks notebook source
# MAGIC %run ../init_notebook

# COMMAND ----------

import booksutilities.bookslibrary as b

# COMMAND ----------

(spark
 .readStream
 .format("cloudFiles")
 .option("cloudFiles.format", 'json')
 .option("cloudFiles.schemaLocation", b.users_pii_checkpoint_raw)
 .load(b.users_pii_path)
 .writeStream
 .format("delta")
 .option("checkpointLocation", b.users_pii_checkpoint_raw)
 .option("mergeSchema", "true")
 .option("path", b.users_pii_bronze_path)
 .trigger(availableNow=True)
 .outputMode("append")
 .table('users_pii_bronze'))

# COMMAND ----------

display(spark.table('users_pii_bronze'))
