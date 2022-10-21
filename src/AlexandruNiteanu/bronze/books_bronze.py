# Databricks notebook source
# notebook for reading the data from the storage and write it as parquet
# used for BX-Books

# COMMAND ----------

# MAGIC %run ../autoloader

# COMMAND ----------

# MAGIC %run ../paths_database

# COMMAND ----------

df_books = autoload(
    f"{storage}Bx-Books/".format("alexandruniteanu"),
    "csv",
    f"{dbx_file_system}books_raw_checkpoint/",
    delimiter=";",
)

# COMMAND ----------

df_books.writeStream.format("delta").option(
    "checkpointLocation",
    f"{dbx_file_system}books_raw_checkpoint/",
).option("path", f"{storage}".format("02parseddata") + "AN_Books/books").trigger(
    availableNow=True
).outputMode(
    "append"
).table(
    "bronze_books"
)
