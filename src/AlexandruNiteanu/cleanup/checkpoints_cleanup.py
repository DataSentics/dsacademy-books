# Databricks notebook source
# notebook for cleaning the checkpoints

# COMMAND ----------

# MAGIC %run ../paths_database

# COMMAND ----------

checkpoints_paths = [
  "books_raw_checkpoint/",
  "raw_books_rating_checkpoint/",
  "piiUsers_raw_checkpoint/"
  "raw_users_checkpoint/"
  "silver_ratings_checkpoint/",
  "books_silver_checkpoint/",
  "silver_piiusers_checkpoint/",
  "usersData_piiInfo_checkpoint/",
  "silver_users_checkpoint/",  
]

# COMMAND ----------

for checkpoint in checkpoints_paths:
    dbutils.fs.rm(
        f"{dbx_file_system}{checkpoint}",
        True,
    )
