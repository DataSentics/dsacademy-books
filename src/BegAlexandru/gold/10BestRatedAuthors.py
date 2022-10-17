# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

# 10 best-rated authors in total

# COMMAND ----------

# MAGIC %run ./AverageBookRating

# COMMAND ----------

joined_df = (
    joined_df.orderBy(col("Book-Rating").desc())
    .limit(10)
)

# COMMAND ----------

joined_df.show()
