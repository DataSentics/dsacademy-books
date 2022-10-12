# Databricks notebook source
# MAGIC %run ../Set_paths/bronze_paths

# COMMAND ----------

# MAGIC %run ../Set_paths/silver_paths

# COMMAND ----------

dbutils.fs.rm(books_checkpoint, True)
dbutils.fs.rm(books_write_path, True)

dbutils.fs.rm(ratings_checkpoint, True)
dbutils.fs.rm(ratings_write_path, True)

dbutils.fs.rm(users_checkpoint, True)
dbutils.fs.rm(users_write_path, True)

dbutils.fs.rm(users_pii_checkpoint, True)
dbutils.fs.rm(users_pii_write_path, True)

# COMMAND ----------

dbutils.fs.rm(books_checkpoint, True)

dbutils.fs.rm(ratings_checkpoint, True)

dbutils.fs.rm(users_checkpoint, True)

dbutils.fs.rm(users_pii_checkpoint, True)

dbutils.fs.rm(users_3nf_checkpoint, True)
