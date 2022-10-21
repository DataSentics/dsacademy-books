# Databricks notebook source
# path for the storage and dbfs(i store the checkpoints here) I used, database I will use, paths for the checkpoints

# COMMAND ----------

# MAGIC %sql
# MAGIC --the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

storage = "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/"
dbx_file_system = "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/"
