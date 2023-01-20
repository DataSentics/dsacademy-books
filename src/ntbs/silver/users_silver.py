# Databricks notebook source
# MAGIC %run ../setup/initial_book_setup

# COMMAND ----------

from pyspark.sql import functions as f
import pipelineutils.pathz as P

# COMMAND ----------

(spark
 .table("users_bronze")
 .select(f.trim(f.col("User-ID")).cast("integer").alias("USER_ID"),
         f.trim(f.split(f.col("Location"), ",").getItem(0)).alias("CITY"),
         f.trim(f.split(f.col("Location"), ",").getItem(1)).alias("REGION"),
         f.trim(f.split(f.col("Location"), ",").getItem(2)).alias("COUNTRY"),
         f.trim(f.when(((f.col("Age") > 122) | (f.col("Age") < 5)), None)
                .otherwise(f.col("Age"))).cast("integer").alias("AGE"),
         f.col("_rescued_data").alias("_rescued_data_users")
         )
 .na.replace({'n/a' : None})
 .na.replace({'' : None})
 .na.replace({0 : None})
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("path", P.silver_users_path)
 .saveAsTable("users_silver")
  )
