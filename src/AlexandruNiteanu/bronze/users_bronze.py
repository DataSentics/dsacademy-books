# Databricks notebook source
# notebook for reading the data from the storage and write it as parquet
# used for BX-Users

# COMMAND ----------

# MAGIC %sql
# MAGIC --the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

# MAGIC %run ../autoloader

# COMMAND ----------

# path for reading the csv
user_path_reading = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/Bx-Users/".format("alexandruniteanu")
)
# path for writing the csv as parquet
user_path_writing = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AN_Books/users"
)

# COMMAND ----------

df = autoload(
    user_path_reading,
    "csv",
    "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/raw_users_checkpoint/",
    delimiter=";",
)

# COMMAND ----------

df.writeStream.format("delta").option(
    "checkpointLocation",
    "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/raw_users_checkpoint/",
).option("path", user_path_writing).outputMode(
    "append"
).table(
    "bronze_users"
)
