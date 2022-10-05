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
    + "books_crossing/"
)
# path for writing the json as parquet
pii_path_writing = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AN_Books/users_pii"
)

# COMMAND ----------

def autoload(data_source, source_format, checkpoint_directory):
    query = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", source_format)
        .option("cloudFiles.schemaLocation", checkpoint_directory)
        .load(data_source)
    )
    return query

# COMMAND ----------

# saving the csv into a df
df = autoload(
    pii_path_reading,
    "json",
    "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/piiUsers_raw_checkpoint1/",
)

# COMMAND ----------

# writing it as parquet in the azure storage
df.writeStream.option(
    "checkpointLocation",
    "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/piiUsers_raw_checkpoint1/",
).option("mergeSchema", "true").option("path", pii_path_writing).outputMode(
    "append"
).format("delta").table(
    "pii_users_raw"
)
