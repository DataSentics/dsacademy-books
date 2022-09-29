# Databricks notebook source
# notebook for reading the data from the storage and write it as parquet
# used for users-pii

# COMMAND ----------

# MAGIC %sql
# MAGIC --the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

# path for reading the json
pii_path_reading = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("01rawdata")
    + "books_crossing/users-pii.json"
)
# path for writing the json as parquet
pii_path_writing = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AN_Books/users_pii"
)

# COMMAND ----------

# saving the json into a df
df = spark.read.option("header", "true").json(pii_path_reading)

# COMMAND ----------

# registering the table in the metastore
df.write.mode("overwrite").saveAsTable("pii_users_raw")


# COMMAND ----------

# writing it as parquet in the azure storage
df.write.parquet(pii_path_writing, mode='overwrite')
