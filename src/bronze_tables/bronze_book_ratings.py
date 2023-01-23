# Databricks notebook source
# MAGIC %md
# MAGIC # Import necessary modules

# COMMAND ----------

import mypackage.mymodule as m

# COMMAND ----------

# MAGIC %md
# MAGIC # Run initial setup

# COMMAND ----------

# MAGIC %run ../use_database

# COMMAND ----------

# MAGIC %md
# MAGIC # Autoload data

# COMMAND ----------

m.autoload_to_table(m.raw_book_ratings_path,
                    'bronze_book_ratings',
                    m.checkpoint_bronze_book_ratings,
                    'csv', 'latin1',
                    m.bronze_book_ratings_path,
                    separator=";")
