# Databricks notebook source
# notebook for reading the data from the storage and write it as parquet
# used for BX-Books-Rating

# COMMAND ----------

# MAGIC %run ../autoloader

# COMMAND ----------

# MAGIC %run ../paths_database

# COMMAND ----------

df_books_rating = autoload(
    books_rating_path_raw,
    "csv",
    books_rating_raw_checkpoint,
    delimiter=";"
)

# COMMAND ----------

df_books_rating.writeStream.format("delta").option(
    "checkpointLocation",
    books_rating_raw_checkpoint,
).option("path", books_rating_path_parsed).outputMode(
    "append"
).table(
    "books_rating_bronze"
)
