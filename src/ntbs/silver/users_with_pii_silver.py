# Databricks notebook source
# MAGIC %run ../setup/initial_book_setup

# COMMAND ----------

from pyspark.sql import functions as f
import pipelineutils.pathz as P

# COMMAND ----------

(spark
 .table("users_pii_bronze")
 .select(f.col("User-ID").cast("integer").alias("USER_ID"),
         f.col("firstName").alias("FIRST_NAME"),
         f.col("middleName").alias("MIDDLE_NAME"),
         f.col("lastName").alias("LAST_NAME"),
         f.col("gender").alias("GENDER"),
         f.col("ssn").alias("SSN"),
         f.col("_rescued_data").alias("_rescued_data_users_pii")
         )
 .createOrReplaceTempView("users_pii_silver_view")
 )

# COMMAND ----------

(spark
 .table("users_silver")
 .join(spark.table("users_pii_silver_view"), "USER_ID", "inner")
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("path", P.silver_users_with_pii_path)
 .saveAsTable("users_with_pii_silver")
 )

# COMMAND ----------

spark.catalog.dropTempView("users_pii_silver_view")
