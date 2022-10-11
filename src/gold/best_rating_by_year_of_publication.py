# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %sql
# MAGIC USE radomirfabian_books

# COMMAND ----------

# MAGIC %run ./book_rating_by_year_of_publication

# COMMAND ----------

joined_df = (
    joined_df.orderBy(col("Book-Rating").desc())
)

# COMMAND ----------

joined_df.createOrReplaceTempView("best_rating_by_year_of_publication")
