# Databricks notebook source
# MAGIC %md
# MAGIC # Import necessary modules

# COMMAND ----------

import mypackage.mymodule as m

# COMMAND ----------

# MAGIC %md
# MAGIC # Run initial setup

# COMMAND ----------

# MAGIC %run ../init_setup

# COMMAND ----------

# MAGIC %md
# MAGIC # Autoload data

# COMMAND ----------

autoload_to_table(m.raw_books_path,
                  'bronze_books',
                  m.checkpoint_bronze_books,
                  'csv', 'latin1',
                  m.bronze_books_path,
                  separator=";")
