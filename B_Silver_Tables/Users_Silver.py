# Databricks notebook source
# MAGIC %run ../initial_notebook

# COMMAND ----------

from pyspark.sql.functions import trim, col, split, when

(spark
 .table("users_bronze")
 .select(trim(col("User-ID")).cast("integer").alias("User-ID"),
         trim(split(col("Location"), ",").getItem(0)).alias("City"),
         trim(split(col("Location"), ",").getItem(1)).alias("County"),
         trim(split(col("Location"), ",").getItem(2)).alias("Country"),
         trim(when(((col("Age") > 122) | (col("Age") < 5)), None)
              .otherwise(col("Age"))).cast("integer").alias("Age")
         )
 .na.replace({'n/a': None})
 .na.replace({'': None})
 .na.replace({0: None})
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("path", users_cleansed_path)
 .saveAsTable("users_silver")
 )
