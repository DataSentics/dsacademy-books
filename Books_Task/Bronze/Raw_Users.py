# Databricks notebook source
# MAGIC %run ../init_notebook

# COMMAND ----------

import booksutilities.bookslibrary as b

# COMMAND ----------

# Parsing raw users table

b.autoload_to_table(b.users_path, 'users_bronze', b.users_checkpoint_raw, b.users_bronze_path)

# COMMAND ----------

display(spark.table('users_bronze'))
