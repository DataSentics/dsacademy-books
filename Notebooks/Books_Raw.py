# Databricks notebook source
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
 .option("cloudFiles.schemaHints", "ISBN STRING ,Title STRING, Author STRING, Year_of_publication INT,Image_S STRING,Image_M STRING, Image_L STRING")
 .option("cloudFiles.schemaLocation",u.books_checkpoint_raw)
 .load(u.books_path)
 .writeStream
 .format("delta")
 .option("checkpointLocation", u.books_checkpoint_raw)
 .option("mergeSchema", "true")
 .option("path", u.books_bronze_path)
 .trigger(availableNow=True)
 .outputMode("append")
 .table('books_bronze')
)


# COMMAND ----------

display(spark.table('books_bronze'))

# COMMAND ----------



# COMMAND ----------


