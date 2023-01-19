# Databricks notebook source
# MAGIC %run ../init_setup 

# COMMAND ----------

autoload_to_table(raw_users_path, 'bronze_users', checkpoint_bronze_users, 'csv', 'latin1', bronze_users_path, separator=";")
