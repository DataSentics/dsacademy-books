# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

# 10 best-rated authors in total

# COMMAND ----------

df_books_with_ratings = spark.table("average_book_rating")

# COMMAND ----------

df_books_with_ratings = (
    df_books_with_ratings.orderBy(col("Book-Rating").desc())
    .limit(10)
)

# COMMAND ----------

df_books_with_ratings.show()
