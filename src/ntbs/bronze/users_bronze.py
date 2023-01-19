# Databricks notebook source
# MAGIC %run ../setup/initial_book_setup

# COMMAND ----------

autoload_to_table(users_path,
                  "users_bronze",
                  bronze_users_checkpoint_path,
                  "csv",
                  "latin1",
                  bronze_users_path,
                  ";")
