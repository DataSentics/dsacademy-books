# Databricks notebook source
# MAGIC %run ../setup/initial_book_setup

# COMMAND ----------

from pyspark.sql import functions as f
import pipelineutils.pathz as P

# COMMAND ----------

(spark
 .table("books_silver")
 .groupBy("PUBLISHER")
 .agg(f.max(f.col("YEAR_OF_PUBLICATION")).alias("LATEST_RELEASE_YEAR"))
 .withColumn("year_date", f.year(f.current_timestamp()))
 .filter((f.col("LATEST_RELEASE_YEAR").isNotNull()) & (f.col("LATEST_RELEASE_YEAR") < f.col("year_date") - 10))
 .drop("year_date")
 .sort(f.col("LATEST_RELEASE_YEAR").desc())
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("path", P.lost_publishers_path)
 .saveAsTable("lost_publishers")
 )
