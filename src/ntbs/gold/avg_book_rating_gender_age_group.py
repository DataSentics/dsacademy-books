# Databricks notebook source
# MAGIC %run ../setup/initial_book_setup

# COMMAND ----------

from pyspark.sql import functions as f
import pipelineutils.pathz as P

# COMMAND ----------

(spark
 .table("book_ratings_silver")
 .join(spark.table("users_with_pii_silver"), "USER_ID", "inner")
 .withColumn("AGE_GROUP", f.concat(f.floor(f.col("AGE") / 10) * 10 + 1, f.lit("-"), (f.floor(f.col("AGE") / 10) * 10 + 10)))
 .groupBy("GENDER", "AGE_GROUP")
 .agg((f.mean(f.col("BOOK_RATING")).alias("AVG_RATING")))
 .filter(f.col("AGE_GROUP").isNotNull())
 .sort("GENDER", f.lit((f.split(f.col("AGE_GROUP"), "-")[0]).cast("int")))
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("path", P.avg_book_rating_gender_age_group_path)
 .saveAsTable("avg_book_rating_gender_age_group")
 )
