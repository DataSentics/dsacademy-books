# Databricks notebook source
# 10 best-rated authors in total

# COMMAND ----------

# MAGIC %run ../paths_database

# COMMAND ----------

from pyspark.sql.functions import col, avg

# COMMAND ----------

df_users_rating_books = spark.table("users_rating_books")

# COMMAND ----------

df_result = (
    df_users_rating_books.groupBy("Book-Author")
    .agg(avg("Book-Rating").alias("Average_Book_Rating"))
    .orderBy(col("Average_Book_Rating").desc())
    .limit(10)
)

# COMMAND ----------

df_result.createOrReplaceTempView("average_book_rating")
