# Databricks notebook source
# notebook for reading the data from the storage and write it as parquet
# used for BX-Books-Rating

# COMMAND ----------

# MAGIC %sql
# MAGIC -- the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

# path for reading the csv
path_reading = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/BX-Book-Ratings/".format("alexandruniteanu")
)
# path for writing the csv as parquet
path_writing = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AN_Books/books_rating"
)

# COMMAND ----------

def autoload(data_source, source_format, checkpoint_directory):
    query = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", source_format)
        .option("cloudFiles.schemaLocation", checkpoint_directory)
        .option("nullValue", None)
        .option("delimiter", ";")
        .option("header", True)
        .load(data_source)
        .createOrReplaceTempView("rating_raw_temp")
    )
    return query

# COMMAND ----------

# saving the csv into a df
df = autoload(
    path_reading,
    "csv",
    "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/books_rating_raw_checkpoint1/",
)

# COMMAND ----------

# registering the table in the metastore and writing it back as parquet to the storage
query = (
    spark.table("rating_raw_temp")
    .writeStream.format("delta")
    .option(
        "checkpointLocation",
        "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/books_rating_raw_checkpoint1/",
    )
    .outputMode("append")
    .table("books_rating_raw")
)
