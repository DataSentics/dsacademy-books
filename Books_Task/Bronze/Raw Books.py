# Databricks notebook source
# MAGIC %run ../Initializing_Notebook

# COMMAND ----------

# Parsing raw books table

autoload_to_table(books_path, 'books_bronze', books_checkpoint_raw, books_bronze_path)

# COMMAND ----------

read_stream('books_bronze')

# COMMAND ----------

# Assigning table to a variable

df = spark.read.table('books_bronze')

# COMMAND ----------

# Checking schema

df.printSchema()

# COMMAND ----------


