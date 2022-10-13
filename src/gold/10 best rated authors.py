# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %sql
# MAGIC USE radomirfabian_books

# COMMAND ----------

# MAGIC %run ./average_book_rating

# COMMAND ----------

joined_df = (
    joined_df.orderBy(col("Book-Rating").desc())
    .limit(10)
)
