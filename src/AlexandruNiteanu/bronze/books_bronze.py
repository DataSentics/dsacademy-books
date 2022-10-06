# Databricks notebook source
# notebook for reading the data from the storage and write it as parquet
# used for BX-Books

# COMMAND ----------

# MAGIC %sql
# MAGIC -- the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

# MAGIC %run ../autoloader

# COMMAND ----------

# path for reading the csv
books_path_reading = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/Bx-Books/".format("alexandruniteanu")
)
# path for writing the csv as parquet
books_path_writing = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AN_Books/books"
)

# COMMAND ----------

# def autoload(data_source, source_format, checkpoint_directory):
#     query = (
#         spark.readStream.format("cloudFiles")
#         .option("cloudFiles.format", source_format)
#         .option("cloudFiles.schemaLocation", checkpoint_directory)
#         .option("nullValue", None)
#         .option("encoding", "iso8859-1")
#         .option("delimiter", ";")
#         .option("header", True)
#         .load(data_source)
#         .createOrReplaceTempView("books_raw_temp")
#     )
#     return query

# COMMAND ----------

df = autoload(books_path_reading, "csv", "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/books_raw_checkpoint2/",delimiter=';')

# COMMAND ----------

df.writeStream.format("delta").option(
    "checkpointLocation",
    "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/books_raw_checkpoint2/",
).option("path", books_path_writing).outputMode(
    "append"
).table(
    "bronze_books"
)
