# Databricks notebook source
import mypackage.mymodule as m

# COMMAND ----------

# MAGIC %run ../init_setup

# COMMAND ----------

autoload_to_table(m.raw_books_path, 
                  'bronze_books', 
                  m.checkpoint_bronze_books, 
                  'csv', 'latin1', 
                  m.bronze_books_path, 
                  separator=";")
