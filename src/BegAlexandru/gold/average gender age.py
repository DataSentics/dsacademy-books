# Databricks notebook source
from pyspark.sql.functions import col, concat, avg, lit

# COMMAND ----------

# A new analytical query: What is the average book rating per gender (men vs. women)
# and per age group (age groups are: 0-10, 11-20, 21-30...)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

Interval_of_age = spark.readStream.table("joined_books")

# COMMAND ----------

Interval_of_age = (
    Interval_of_age.withColumn("Interval", col("Age") - (col("Age") % 10))
    .withColumn(
        "Interval", concat(col("Interval") + 1, lit(" - "), col("Interval") + 10)
    )
    .groupBy("Interval", "gender")
    .agg(avg("Book-Rating").alias("Average_Book_Rating"))
)
