# Databricks notebook source
# MAGIC %run ../setup/initial_book_setup

# COMMAND ----------

from pyspark.sql import functions as f
import pipelineutils.pathz as P

# COMMAND ----------

(spark
 .table("books_silver")
 .join(spark.table("book_ratings_silver"), "ISBN", "inner")
 .join(spark.table("users_silver"), "USER_ID", "inner")
 .filter(f.col("YEAR_OF_PUBLICATION") > 2000)
 .groupBy("USER_ID", "COUNTRY")
 .agg(f.count("BOOK_RATING").alias("NO_OF_BOOKS_RATED"))
 .sort(f.col("NO_OF_BOOKS_RATED").desc())
 .limit(1)
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("path", P.user_with_most_ratings_after_2000_path)
 .saveAsTable("user_with_most_ratings_after_2000")
 )
