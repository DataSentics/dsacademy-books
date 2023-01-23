# Databricks notebook source
# MAGIC %run ../initial_notebook

# COMMAND ----------

from pyspark.sql.functions import col


def best_book_by_period(df, start_year, end_year):
    ratings_count_per_book = df.groupBy('ISBN').count()
    df = df.where((col('Year-Of-Publication') > start_year)
                  & (col('Year-Of-Publication') < end_year))
    result = df.groupBy('ISBN', 'Book-Title',
                        'Year-OF-Publication').avg('Book-Rating')
    return result.join(ratings_count_per_book, 'ISBN').sort(['avg(Book-Rating)', 'count'], ascending=[False, False])


books_data_frame = spark.table("books_silver").join(
    spark.table("book_ratings_silver"), "ISBN")

display(best_book_by_period(books_data_frame, 2000, 2015))
