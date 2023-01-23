# Databricks notebook source
# MAGIC %run ../init_notebook

# COMMAND ----------

import booksutilities.bookslibrary as b

# COMMAND ----------

# Parsing raw users table

b.autoload_to_table(b.users_pii_path, 'users_pii_bronze',
                    b.users_pii_checkpoint_raw, b.users_pii_bronze_path, 'json', ';')

# COMMAND ----------

display(spark.table('users_pii_bronze'))
