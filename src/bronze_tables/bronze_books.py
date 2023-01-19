# Databricks notebook source
# MAGIC %run ../init_setup 

# COMMAND ----------

autoload_to_table(raw_books_path, 'bronze_books', checkpoint_bronze_books, 'csv', 'latin1', bronze_books_path, separator=";")
