# Databricks notebook source
# MAGIC %run ../Initializing_Notebook

# COMMAND ----------

# Parsing raw users table

autoload_to_table(users_path, 'users_bronze', users_checkpoint_raw, users_bronze_path)

# COMMAND ----------

read_stream('users_bronze')

# COMMAND ----------

# Assigning table to a variable

df = spark.read.table('users_bronze')

# COMMAND ----------

# Checking schema

df.printSchema()
