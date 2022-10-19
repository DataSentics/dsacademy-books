# Databricks notebook source
# MAGIC %run ./variables

# COMMAND ----------

list_checkpoints = [
    "daniela_books_raw_checkpoint/",
    "daniela_books_ratings_raw_checkpoint/",
    "daniela_users_raw_checkpoint/",
    "daniela_users_pii_r_checkpoint/",
    "daniela_users_joined_pii_checkpoint/",
    "daniela_books_ratings_silver_checkpoint/",
    "daniela_books_silver_checkpoint/",
    "daniela_users_pii_silver_checkpoint/",
    "daniela_users_silver_checkpoint/",
]

# COMMAND ----------

for checkpoint in list_checkpoints:
    dbutils.fs.rm(
        f"{working_dir}{checkpoint}",
        True,
    )
