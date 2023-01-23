# Databricks notebook source
# MAGIC %run ../initial_notebook

# COMMAND ----------

autoload_to_table(ratings_source, "book_ratings_bronze", ratings_checkpoint_raw,
                  ratings_parsed_path, source_format='csv', delimiter=';')

# COMMAND ----------

display(spark.table("book_ratings_bronze"))
