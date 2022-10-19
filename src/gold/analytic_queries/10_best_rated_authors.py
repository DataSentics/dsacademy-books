# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %sql
# MAGIC USE radomirfabian_books

# COMMAND ----------

df = spark.table("gold_avg_book_rating")

# COMMAND ----------

df = (
    df.orderBy(col("Book-Rating").desc())
    .limit(10)
)
