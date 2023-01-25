# Databricks notebook source
# MAGIC %md
# MAGIC # Import necessary modules

# COMMAND ----------

import mypackage.mymodule as m

# COMMAND ----------

# MAGIC %run ../use_database

# COMMAND ----------

# MAGIC %md
# MAGIC # Autoload data

# COMMAND ----------

(spark
 .readStream
 .format("cloudFiles")
 .option("sep", ';')
 .option("header", True)
 .option("encoding", 'latin1')
 .option("cloudFiles.format", 'csv')
 .option("cloudFiles.schemaLocation", m.checkpoint_bronze_users)
 .load(m.raw_users_path)
 .writeStream
 .outputMode('append')
 .option("checkpointLocation", m.checkpoint_bronze_users)
 .option("mergeSchema", "true")
 .trigger(once=True)
 .option('path', m.bronze_users_path)
 .table('bronze_users'))
