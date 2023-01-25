# Databricks notebook source
# MAGIC %run ../setup/initial_book_setup

# COMMAND ----------

import pipelineutils.paths as P
import pipelineutils.agg_functions as AF

# COMMAND ----------

(AF.best_10_books_by_sex_age_country("M", [10, 20], "usa", 10)
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("path", P.top_10_rated_books_agesexcountry_path)
 .saveAsTable("top_10_rated_books_agesexcountry")
 )
