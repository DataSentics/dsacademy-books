# Databricks notebook source
# MAGIC %run ../setup/initial_book_setup

# COMMAND ----------

from pyspark.sql import functions as f
import pipelineutils.pathz as P

# COMMAND ----------

(spark
 .table("books_silver")
 .groupby("BOOK_AUTHOR").count()
 .sort(f.col("count").desc())
 .withColumnRenamed("count", "TITLES_PUBLISHED")
 .limit(50)
 .join(spark.table("books_silver"), "BOOK_AUTHOR", "inner")
 .join(spark.table("book_ratings_silver"), "ISBN", "inner")
 .groupBy("BOOK_AUTHOR", "TITLES_PUBLISHED")
 .agg(f.mean(f.col("BOOK_RATING")).alias("AVG_RATING"))
 .sort(f.col("AVG_RATING").desc())
 .limit(10)
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("path", P.top_10_authors_titles_weighted_path)
 .saveAsTable("top_10_authors_titles_weighted")
 )
