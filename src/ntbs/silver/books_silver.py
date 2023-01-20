# Databricks notebook source
# MAGIC %run ../setup/initial_book_setup

# COMMAND ----------

from pyspark.sql import functions as f
import pipelineutils.pathz as P

# COMMAND ----------

(spark
 .table("books_bronze")
 .withColumn("ISBN", f.substring("ISBN", 1, 10))
 .drop("Image-URL-S", "Image-URL-M", "Image-URL-L")
 .select(f.trim(f.col("ISBN")).alias("ISBN"),
         f.trim(f.col("Book-Title")).alias("BOOK_TITLE"),
         f.trim(f.col("Book-Author")).alias("BOOK_AUTHOR"),
         f.trim(f.col("Year-Of-Publication")).cast("integer").alias("YEAR_OF_PUBLICATION"),
         f.trim(f.col("Publisher")).alias("PUBLISHER"),
         f.col("_rescued_data").alias("_rescued_data_books")
         )
 .withColumn("YEAR_OF_PUBLICATION",
             f.when((f.col("YEAR_OF_PUBLICATION") == 0) | (f.col("YEAR_OF_PUBLICATION") > 2023),
                    None).otherwise(f.col("YEAR_OF_PUBLICATION"))
             )
 .dropDuplicates(["ISBN"])
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("path", P.silver_books_path)
 .saveAsTable("books_silver")
 )
