# Databricks notebook source
import mypackage.mymodule as m

# COMMAND ----------

# MAGIC %run ../init_setup

# COMMAND ----------

autoload_to_table(m.raw_book_ratings_path, 
                  'bronze_book_ratings', 
                  m.checkpoint_bronze_book_ratings, 
                  'csv', 'latin1', 
                  m.bronze_book_ratings_path, 
                  separator=";")
