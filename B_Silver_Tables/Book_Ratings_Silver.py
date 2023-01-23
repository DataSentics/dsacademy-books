# Databricks notebook source
# MAGIC %run ../initial_notebook

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE book_ratings_bronze SET TBLPROPERTIES (
# MAGIC    'delta.columnMapping.mode' = 'name',
# MAGIC    'delta.minReaderVersion' = '2',
# MAGIC    'delta.minWriterVersion' = '5')

# COMMAND ----------

from pyspark.sql.functions import trim, col

(spark
 .table("book_ratings_bronze")
 .filter(col("Book-Rating") > 0)
 .select(
     trim(col("User-ID").cast("integer")),
     trim(col("ISBN")),
     trim(col("Book-Rating").cast("integer")))
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("path", ratings_cleansed_path)
 .saveAsTable("book_ratings_silver"))

# COMMAND ----------

df = spark.table("book_ratings_silver")
display(df)
