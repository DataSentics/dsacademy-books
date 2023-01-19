# Databricks notebook source
import mypackage.mymodule as m

# COMMAND ----------

# MAGIC %run ../init_setup

# COMMAND ----------

autoload_to_table(m.raw_users_pii_path, 'bronze_users_pii', m.checkpoint_bronze_users_pii, 'json', 'latin1', m.bronze_users_pii_path, separator=";")
