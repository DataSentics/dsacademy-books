# Databricks notebook source
# import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

df_users_pii = spark.table("users_pii_bronze")
