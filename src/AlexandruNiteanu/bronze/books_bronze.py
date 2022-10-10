# Databricks notebook source
# notebook for reading the data from the storage and write it as parquet
# used for BX-Books

# COMMAND ----------

# MAGIC %run ../autoloader

# COMMAND ----------

# MAGIC %run ../paths_database

# COMMAND ----------

df_books = autoload(
    books_path_raw,
    "csv",
    books_checkpoint_raw,
    delimiter=";",
)

# COMMAND ----------

df_books.writeStream.format("delta").option(
    "checkpointLocation",
    books_checkpoint_raw,
).option("path", books_path_parsed).outputMode(
    "append"
).table(
    "bronze_books"
)
