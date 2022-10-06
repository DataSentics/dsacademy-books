# Databricks notebook source
# notebook for reading the data from the storage and write it as parquet
# used for users-pii

# COMMAND ----------

# MAGIC %sql
# MAGIC --the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

# MAGIC %run ../autoloader

# COMMAND ----------

# path for reading the json
pii_path_reading = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("01rawdata")
    + "books_crossing/"
)
# path for writing the json as parquet
pii_path_writing = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AN_Books/users_pii"
)

# COMMAND ----------

# saving the json into a df
df = autoload(
    pii_path_reading,
    "json",
    "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/piiUsers_raw_checkpoint3/",
    delimiter=','
)

# COMMAND ----------

df.writeStream.format("parquet").option(
    "checkpointLocation",
    "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/userspii_raw_checkpoint3/",
).option("path", pii_path_writing).outputMode(
    "append"
).table(
    "bronze_users_pii"
)
