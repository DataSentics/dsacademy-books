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
 .option("sep", ";")
 .option("header", True)
 .option("encoding", 'latin1')
 .option("cloudFiles.format", 'csv')
 .option("cloudFiles.schemaLocation", m.checkpoint_bronze_book_ratings)
 .load(m.raw_book_ratings_path)
 .writeStream
 .outputMode('append')
 .option("checkpointLocation", m.checkpoint_bronze_book_ratings)
 .option("mergeSchema", "true")
 .trigger(once=True)
 .option('path', m.bronze_book_ratings_path)
 .table('bronze_book_ratings'))
