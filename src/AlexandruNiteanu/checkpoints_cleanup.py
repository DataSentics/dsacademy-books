# Databricks notebook source
# notebook for cleaning the checkpoints

# COMMAND ----------

# MAGIC %run ../paths_database

# COMMAND ----------

# user checkpoints
dbutils.fs.rm(users_raw_checkpoint)
dbutils.fs.rm(users_3nf_checkpoint)
dbutils.fs.rm(users_checkpoint)

# COMMAND ----------

# books checkpoints
dbutils.fs.rm(books_checkpoint_raw)
dbutils.fs.rm(books_checkpoint)

# COMMAND ----------

# book rating checkpoints
dbutils.fs.rm(books_rating_raw_checkpoint)
dbutils.fs.rm(books_rating_checkpoint)

# COMMAND ----------

# user_pii checkpoints
dbutils.fs.rm(pii_users_raw_checkpoint)
dbutils.fs.rm(pii_users_checkpoint)
