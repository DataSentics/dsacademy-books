# Databricks notebook source
# MAGIC %run ../setup/initial_book_setup

# COMMAND ----------

import pipelineutils.pathz as P
import pipelineutils.agg_functions as AF

# COMMAND ----------

(AF.best_rated_popular_books_after()
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("path", P.top_10_rated_books_after_2000_path)
 .saveAsTable("top_10_rated_books_after_2000")
 )
