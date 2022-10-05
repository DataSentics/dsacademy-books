# Databricks notebook source
spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

book_rating_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("01rawdata")
    + "books_crossing/BX-Book-Ratings.csv"
)

df_books_ratings = (
    spark.read.option("sep", ";")
    .option("header", True)
    .option("inferSchema", True)
    .option("encoding", "ISO-8859-1")
    .csv(book_rating_path)
)

df_books_ratings.createOrReplaceTempView("books_ratings_bronze_tempView")
spark.sql(
    "CREATE OR REPLACE TABLE books_ratings_bronze AS SELECT * FROM books_ratings_bronze_tempView"
)

# COMMAND ----------

books_ratings_path_upload = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "daniela-vlasceanu-books/bronze/books-ratings"
)

df_books_ratings.write.parquet(books_ratings_path_upload, mode="overwrite")
