# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %sql
# MAGIC USE radomirfabian_books

# COMMAND ----------

df = spark.table("gold_book_rating_by_year_of_pub")

# COMMAND ----------

df = (
    df.orderBy(col("Book-Rating").desc())
)

# COMMAND ----------

display(df)
