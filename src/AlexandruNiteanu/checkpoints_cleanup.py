# Databricks notebook source
# notebook for cleaning the checkpoints

# COMMAND ----------

# MAGIC %run ./paths_database

# COMMAND ----------

# user checkpoints
dbutils.fs.rm(users_raw_checkpoint, True)
dbutils.fs.rm(usersData_piiInfo_checkpoint, True)
dbutils.fs.rm(users_checkpoint, True)

# COMMAND ----------

# books checkpoints
dbutils.fs.rm(books_checkpoint_raw, True)
dbutils.fs.rm(books_checkpoint, True)

# COMMAND ----------

# book rating checkpoints
dbutils.fs.rm(books_rating_raw_checkpoint, True)
dbutils.fs.rm(books_rating_checkpoint, True)

# COMMAND ----------

# user_pii checkpoints
dbutils.fs.rm(pii_users_raw_checkpoint, True)
dbutils.fs.rm(pii_users_checkpoint, True)
