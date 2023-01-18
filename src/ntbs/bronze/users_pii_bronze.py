# Databricks notebook source
# MAGIC %run ../setup/initial_book_setup

# COMMAND ----------

autoload_to_table(users_pii_path, "users_pii_bronze", bronze_users_pii_checkpoint_path, "json", "latin1", bronze_users_pii_path, ";")
