# Databricks notebook source
# MAGIC %run ./includes_bronze

# COMMAND ----------

# MAGIC %run ./includes_silver

# COMMAND ----------

# bronze books
dbutils.fs.rm(checkpoint_books_path, True)
dbutils.fs.rm(checkpoint_write_books_path, True)
# silver books
dbutils.fs.rm(checkpoint_books_path, True)

# COMMAND ----------

# bronze users
dbutils.fs.rm(checkpoint_users_path, True)
dbutils.fs.rm(checkpoint_write_users_path, True)
# silver users
dbutils.fs.rm(checkpoint_users_path, True)

# COMMAND ----------

# bronze users-pii
dbutils.fs.rm(checkpoint_users_pii_path, True)
dbutils.fs.rm(checkpoint_write_users_pii_path, True)
# silver users-pii
dbutils.fs.rm(checkpoint_pii_path, True)

# COMMAND ----------

# silver 3nf
dbutils.fs.rm(checkpoint_3nf_path, True)