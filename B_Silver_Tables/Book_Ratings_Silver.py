# Databricks notebook source
# MAGIC %run ../initial_notebook

# COMMAND ----------

from pyspark.sql.functions import trim, col

(spark.table("book_ratings_bronze")
 .where(col("Book-Rating") != 0)
 .select(["User-ID", "ISBN", "Book-Rating"])
 .withColumn("User-ID", col('User-ID').cast("integer"))
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("path", ratings_cleansed_path)
 .saveAsTable("book_ratings_silver"))

# COMMAND ----------

df = spark.table("book_ratings_silver")
display(df)

# COMMAND ----------

df.printSchema()
