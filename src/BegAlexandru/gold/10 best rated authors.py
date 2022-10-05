# Databricks notebook source
from pyspark.sql.functions import avg, col

# COMMAND ----------

# 10 best-rated authors in total

# COMMAND ----------

# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

joined_df = spark.sql("SELECT * FROM joined_books")

# COMMAND ----------

display(
    joined_df.groupBy("Book-Author")
    .agg(avg("Book-Rating").alias("Book-Rating"))
    .orderBy(col("Book-Rating").desc())
    .limit(10)
)
