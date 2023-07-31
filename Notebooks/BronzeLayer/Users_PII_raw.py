# Databricks notebook source
# MAGIC %run /Repos/Book_Task/dsacademy-books/utilities/db_notebook

# COMMAND ----------

import utilities.utilities as u

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", 'json')
 .option("cloudFiles.schemaLocation", u.users_pii_checkpoint_bronze)
 .load(u.users_pii_path)
 .writeStream
 .format("delta")
 .option("checkpointLocation", u.users_pii_checkpoint_bronze)
 .option("mergeSchema", "true")
 .option("path", u.users_pii_bronze_path)
 .trigger(availableNow=True)
 .outputMode("append")
 .table('users_pii_bronze'))

# COMMAND ----------

display(spark.table('users_pii_bronze'))
