# Databricks notebook source
spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

df_books = spark.table("books_silver")
df_books_ratings = spark.table("books_ratings_silver")
# display(df_books)
# display(df_books_ratings)

books_joined = df_books.join(df_books_ratings, "ISBN")

# COMMAND ----------

books_joined.createOrReplaceTempView("books_joined_tempView")
spark.sql(
    "CREATE OR REPLACE TABLE books_joined_silver AS SELECT * FROM books_joined_tempView"
)
