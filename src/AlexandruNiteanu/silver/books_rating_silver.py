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
    books_rating_checkpoint,
).option("path", books_rating_path_cleansed).outputMode("append").table("silver_ratings")
