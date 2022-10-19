# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

# 10 best-rated authors by year of publication

# COMMAND ----------

# MAGIC %run ../gold/book_rating_by_publication_year

# COMMAND ----------

df_books_with_ratings = (
    df_books_with_ratings.orderBy(col("Book-Rating").desc())
)

# COMMAND ----------

df_books_with_ratings.show()
