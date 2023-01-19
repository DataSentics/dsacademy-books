# Databricks notebook source
# MAGIC %run ../init_setup 

# COMMAND ----------

autoload_to_table(raw_book_ratings_path, 'bronze_book_ratings', checkpoint_bronze_book_ratings, 'csv', 'latin1', bronze_book_ratings_path, separator=";")
