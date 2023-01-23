# Databricks notebook source
# MAGIC %run ../setup/initial_book_setup

# COMMAND ----------

from pyspark.sql import functions as f
import pipelineutils.pathz as P

# COMMAND ----------

(spark
 .table("books_silver")
 .join(spark.table("book_ratings_silver"), "ISBN", "inner")
 .groupBy("BOOK_AUTHOR").count()
 .orderBy(f.col("count").desc())
 .withColumnRenamed("count", "RATINGS")
 .limit(50)
 .join(spark.table("books_silver"), "BOOK_AUTHOR", "inner")
 .join(spark.table("book_ratings_silver"), "ISBN", "inner")
 .groupBy("BOOK_AUTHOR", "RATINGS")
 .agg(f.mean(f.col("BOOK_RATING")).alias("AVG_RATING"))
 .sort(f.col("AVG_RATING").desc())
 .limit(10)
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("path", P.top_10_authors_ratings_weighted_path)
 .saveAsTable("top_10_authors_ratings_weighted")
 )
