# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Author : Boboescu Denis
# MAGIC
# MAGIC # Goal : Build the Auto loader for books data
# MAGIC
# MAGIC Obtain an initial streaming DataFrame from using the Auto Loader

# COMMAND ----------

# MAGIC %run ./Utilities/db_notebook

# COMMAND ----------

import Utilities.utilities as u

# COMMAND ----------

# Testing if the Auto Loader works

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", 'csv')
 .option("sep", ';')
 .option("encoding", "latin1")
 .option("header", True)
 .option("cloudFiles.schemaLocation",u.books_checkpoint_bronze)
 .load(u.books_path)
 .writeStream
 .format("delta")
 .option("checkpointLocation", u.books_checkpoint_bronze)
 .option("mergeSchema", "true")
 .option("path", u.books_bronze_path)
 .trigger(availableNow=True)
 .outputMode("append")
 .table('books_bronze')
)


# COMMAND ----------

display(spark.table('books_bronze'))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE books_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN denis_boboescu_books;

# COMMAND ----------


