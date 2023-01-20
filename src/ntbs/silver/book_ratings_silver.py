# Databricks notebook source
# MAGIC %run ../setup/initial_book_setup

# COMMAND ----------

from pyspark.sql import functions as f
import pipelineutils.pathz as P

# COMMAND ----------

(spark
 .table("book_ratings_bronze")
 .withColumn("ISBN", f.translate("ISBN", '!"#$%&\'()*+,-./:;<=>?@[\\]^_{|}~', ""))
 .filter(f.col("Book-Rating") > 0)
 .select((f.col("User-ID").cast("integer").alias("USER_ID")),
         f.trim(f.col("ISBN")).alias("ISBN"),
         (f.col("Book-Rating").cast("integer").alias("BOOK_RATING")),
         f.col("_rescued_data").alias("_rescued_data_book_ratings")
        )
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("path", P.silver_book_ratings_path)
 .saveAsTable("book_ratings_silver")
)
