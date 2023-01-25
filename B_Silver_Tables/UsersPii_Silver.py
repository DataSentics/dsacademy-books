# Databricks notebook source
# MAGIC %run ../initial_notebook

# COMMAND ----------

from pyspark.sql.functions import col

(spark
 .table("users_pii_bronze")
 .select(col("User-ID").cast("integer"),
         "firstName",
         "middleName",
         "lastName",
         "gender",
         "ssn",
         )
 .createOrReplaceTempView("users_pii_silver_temp_view")
 )

# COMMAND ----------

(spark
 .table("users_silver")
 .join(spark.table("users_pii_silver_temp_view"), "User-ID", how="inner")
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("path", users_pii_cleansed_path)
 .saveAsTable("users_pii_silver")
 )
