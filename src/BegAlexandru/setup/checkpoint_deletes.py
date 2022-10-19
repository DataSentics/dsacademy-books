# Databricks notebook source
# This notebook deletes  the checkpoint by running includes_bronze and
# includes_silver where there are the checkpoint paths created by the bronze, silver layer

# COMMAND ----------

# MAGIC %run ./includes_bronze

# COMMAND ----------

# MAGIC %run ./includes_silver

# COMMAND ----------

list_of_checkpoints = [
    checkpoint_books_path,
    checkpoint_write_books_path,
    checkpoint_books_path,
    checkpoint_ratings_path,
    checkpoint_write_ratings_path,
    checkpoint_ratings_path,
    checkpoint_users_path,
    checkpoint_write_users_path,
    checkpoint_users_path,
    checkpoint_users_pii_path,
    checkpoint_write_users_pii_path,
    checkpoint_3nf_path
]

# COMMAND ----------

for i in list_of_checkpoints:
    dbutils.fs.rm(i, True)
