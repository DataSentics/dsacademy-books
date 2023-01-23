# Databricks notebook source
# MAGIC %run ../initial_notebook

# COMMAND ----------

autoload_to_table2(users_source, "users_bronze", users_checkpoint_raw,
                   users_parsed_path, source_format='csv', delimiter=';')
