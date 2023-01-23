# Databricks notebook source
# MAGIC %run ../init_notebook

# COMMAND ----------

import booksutilities.bookslibrary as b

# COMMAND ----------

# Parsing raw ratings table

b.autoload_to_table(b.ratings_path, 'ratings_bronze', b.ratings_checkpoint_raw, b.ratings_bronze_path)

# COMMAND ----------

display(spark.table('ratings_bronze'))
