# Databricks notebook source
# MAGIC %run ../init_notebook

# COMMAND ----------

import booksutilities.bookslibrary as b

# COMMAND ----------

# Parsing raw books table

b.autoload_to_table(b.books_path, 'books_bronze', b.books_checkpoint_raw, b.books_bronze_path)

# COMMAND ----------

display(spark.table('books_bronze'))
