# Databricks notebook source
# MAGIC %run ../init_notebook

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install isbnlib

# COMMAND ----------

# Importing libraries and creating UDF from ISBN library in order to be applied as a cleaning filter

from isbnlib import is_isbn10, is_isbn13
import booksutilities.bookslibrary as b
from pyspark.sql import functions as f
from pyspark.sql import types as t

is_valid_isbn = udf(lambda x: is_isbn10(x) or is_isbn13(x), t.BooleanType())

# COMMAND ----------

# Creating a dataframe containing the ratings_bronze table

ratings_bronze = spark.read.format('delta').load(b.ratings_bronze_path)

# COMMAND ----------

# Displaying ratings_bronze

display(ratings_bronze)

# COMMAND ----------

# Cleaning ratings_bronze

ratings_silver = (ratings_bronze
                  .withColumnRenamed('User-ID', 'User_ID')
                  .withColumnRenamed('Book-Rating', 'Book_Rating')
                  .withColumn('ISBN', f.regexp_replace(f.col('ISBN'), '[^0-9X]', ''))
                  .withColumn('User_ID', f.col('User_ID').cast('integer'))
                  .withColumn('Book_Rating', f.col('Book_Rating').cast('integer'))
                  .filter(f.col('Book_Rating') != 0)
                  .filter(is_valid_isbn(f.col("ISBN")))
                  .drop('_rescued_data'))

# COMMAND ----------

# Displaying ratings_silver

display(ratings_silver)
ratings_silver.count()
ratings_silver.printSchema()

# COMMAND ----------

# Saving ratings_silver to path

ratings_silver.write.format('delta').mode('overwrite').save(b.ratings_silver_path)
