# Databricks notebook source
# MAGIC %run ../initial_notebook

# COMMAND ----------

autoload_to_table2(users_pii_source, "users_pii_bronze", users_pii_checkpoint_raw,
                   users_pii_parsed_path, source_format='json')
