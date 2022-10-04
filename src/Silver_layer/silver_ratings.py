# Databricks notebook source
from pyspark.sql.functions import when, col

# COMMAND ----------

# MAGIC %sql
# MAGIC USE andrei_tugmeanu_books

# COMMAND ----------

books_rating_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AT_books/Bronze/books_ratings"
)

# COMMAND ----------

df_book_ratings = (spark.read.parquet(books_rating_path))

# COMMAND ----------

df_book_ratings = (
    spark.read.parquet(books_rating_path)
    .withColumnRenamed("User-ID", "User_ID")
    .withColumnRenamed("Book-Rating", "Book_Rating")
    .withColumn("Book_Rating", col("Book_Rating").cast("Integer"))
)

# COMMAND ----------

df_book_ratings.write.mode('overwrite').saveAsTable("silver_books_ratings")

# COMMAND ----------

output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AT_books/Silver/books_ratings"
)

# COMMAND ----------

df_book_ratings.write.parquet(output_path, mode='overwrite')
