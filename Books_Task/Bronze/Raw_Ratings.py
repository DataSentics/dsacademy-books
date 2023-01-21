# Databricks notebook source
# MAGIC %run ../Initializing_Notebook

# COMMAND ----------

# Parsing raw ratings table

autoload_to_table(ratings_path, 'ratings_bronze', ratings_checkpoint_raw, ratings_bronze_path)

# COMMAND ----------

read_stream('ratings_bronze')

# COMMAND ----------

# Assigning table to a variable

df = spark.read.table('ratings_bronze')

# COMMAND ----------

# Checking schema

df.printSchema()
