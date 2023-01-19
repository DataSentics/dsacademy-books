# Databricks notebook source
# MAGIC %run ../Initializing_Notebook

# COMMAND ----------

# Parsing raw users table

autoload_to_table(users_pii_path, 'users_pii_bronze', users_pii_checkpoint_raw, users_pii_bronze_path, 'json', ';')

# COMMAND ----------

read_stream('users_pii_bronze')

# COMMAND ----------

# Assigning table to a variable

users_pii_bronze = spark.read.table('users_pii_bronze')

# COMMAND ----------

# Checking schema

df.printSchema()
