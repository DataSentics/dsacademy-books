# Databricks notebook source
# MAGIC %run ../init_setup 

# COMMAND ----------

autoload_to_table(raw_users_pii_path, 'bronze_users_pii', checkpoint_bronze_users_pii, 'json', 'latin1', bronze_users_pii_path, separator=";")
