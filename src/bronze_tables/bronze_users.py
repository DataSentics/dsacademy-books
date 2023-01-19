# Databricks notebook source
import mypackage.mymodule as m

# COMMAND ----------

# MAGIC %run ../init_setup

# COMMAND ----------

autoload_to_table(m.raw_users_path, 'bronze_users', m.checkpoint_bronze_users, 'csv', 'latin1', m.bronze_users_path, separator=";")
