# Databricks notebook source
# MAGIC %run ./Utilities/db_notebook

# COMMAND ----------

import Utilities.utilities as u

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", 'csv')
 .option("sep", ';')
 .option("encoding", "latin1")
 .option("header", True)
 .option("cloudFiles.schemaLocation",u.users_checkpoint_bronze)
 .load(u.users_path)
 .writeStream
 .format("delta")
 .option("checkpointLocation", u.users_checkpoint_bronze)
 .option("mergeSchema", "true")
 .option("path", u.users_bronze_path)
 .trigger(availableNow=True)
 .outputMode("append")
 .table('users_bronze')
)

# COMMAND ----------

display(spark.table('users_bronze'))

# COMMAND ----------


