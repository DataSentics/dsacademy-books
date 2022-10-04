# Databricks notebook source
from pyspark.sql.functions import col, avg

# COMMAND ----------

# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

joined_df = spark.sql("SELECT * FROM joined_books")

# COMMAND ----------

# best-rated authors by year of publication and publishers
display(joined_df
        .groupBy("Year-Of-Publication", "Publisher", "Book-Author")
        .agg(avg("Book-Rating").alias("Book-Rating"))
        .orderBy(col("Book-Rating").desc())
        )
