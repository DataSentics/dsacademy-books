# Databricks notebook source
# notebook for reading the data from the storage and write it as parquet
# used for BX-Books-Rating

# COMMAND ----------

# MAGIC %sql
# MAGIC -- the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

# MAGIC %run ../autoloader

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

# saving the csv into a df
df = autoload(
    path_reading,
    "csv",
    "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/raw_books_rating_checkpoint/",
    delimiter=";"
)

# COMMAND ----------

df.writeStream.format("delta").option(
    "checkpointLocation",
    "dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/raw_books_rating_checkpoint/",
).option("path", path_writing).outputMode(
    "append"
).table(
    "books_rating_bronze"
)
