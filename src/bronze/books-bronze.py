# Databricks notebook source
spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

books_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("01rawdata")
    + "books_crossing/BX-Books.csv"
)

df_books = (
    spark.read.option("sep", ";")
    .option("header", True)
    .option("inferSchema", True)
    .option("encoding", "ISO-8859-1")
    .csv(books_path)
)

df_books.createOrReplaceTempView("books_bronze_tempView")
spark.sql("CREATE OR REPLACE TABLE books_bronze AS SELECT * FROM books_bronze_tempView")

# COMMAND ----------

books_path_upload = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "daniela-vlasceanu-books/bronze/books"
)

df_books.write.parquet(books_path_upload, mode="overwrite")
