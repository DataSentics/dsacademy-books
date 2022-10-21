# Databricks notebook source
# notebook for cleaning the data
# used for books_rating file

# COMMAND ----------

from pyspark.sql.functions import when, col

# COMMAND ----------

# MAGIC %run ../paths_database

# COMMAND ----------

# the col Book-Rating was full of 0 so I replaced them with null
df_book_rating = (
    spark.readStream.table("books_rating_bronze")
    .withColumn("Book-Rating", col("Book-Rating").cast("Integer"))
    .withColumn(
        "Book-Rating", when(col("Book-Rating") == 0, None).otherwise(col("Book-Rating"))
    )
)

# COMMAND ----------

df_book_rating.writeStream.format("delta").option(
    "checkpointLocation",
    f"{dbx_file_system}silver_ratings_checkpoint/",
).option("path", f"{storage}".format("03cleanseddata")
    + "AN_Books/books_rating_silver").trigger(availableNow=True).outputMode("append").table("silver_ratings")
