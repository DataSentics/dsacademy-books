# Databricks notebook source
# notebook for reading the data from the storage and write it as parquet
# used for BX-Books-Rating

# COMMAND ----------

# MAGIC %run ../autoloader

# COMMAND ----------

# MAGIC %run ../paths_database

# COMMAND ----------

df_books_rating = autoload(
    f"{storage}BX-Book-Ratings/".format("alexandruniteanu"),
    "csv",
    f"{dbx_file_system}raw_books_rating_checkpoint/",
    delimiter=";",
)

# COMMAND ----------

df_books_rating.writeStream.format("delta").option(
    "checkpointLocation",
    f"{dbx_file_system}raw_books_rating_checkpoint/"
).option("path",f"{storage}".format("02parseddata")
    + "AN_Books/books_rating").trigger(availableNow=True).outputMode(
    "append"
).table(
    "books_rating_bronze"
)
