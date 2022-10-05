# Databricks notebook source
# notebook for reading the data from the storage and write it as parquet
# used for BX-Users

# COMMAND ----------

# MAGIC %sql
# MAGIC --the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

# path for reading the csv
user_path_reading = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("alexandruniteanu")
    + "BX-Users.csv"
)
# path for writing the csv as parquet
user_path_writing = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AN_Books/users"
)

# COMMAND ----------

def autoload(data_source, source_format, checkpoint_directory):
    query = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", source_format)
        .option("cloudFiles.schemaLocation", checkpoint_directory)
        .option("nullValue", None)
        .option("encoding", "iso8859-1")
        .option("delimiter", ";")
        .option("header", True)
        .load(data_source)
    )
    return query

# COMMAND ----------

df = autoload(user_path_reading, "csv", "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/users_raw_checkpoint1/")

# COMMAND ----------

# registering the table in the metastore
df.write.mode("overwrite").saveAsTable("users_raw")

# COMMAND ----------

# writing it as parquet in the azure storage
df.write.parquet(user_path_writing, mode='overwrite')
