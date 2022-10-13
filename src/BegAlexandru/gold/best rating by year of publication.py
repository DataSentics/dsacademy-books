# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

# 10 best-rated authors by year of publication

# COMMAND ----------

# MAGIC %run ./book_rating_by_year_of_publication

# COMMAND ----------

joined_df = (
    joined_df.orderBy(col("Book-Rating").desc())
)

# COMMAND ----------

joined_df.show()
