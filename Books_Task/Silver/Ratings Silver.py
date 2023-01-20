# Databricks notebook source
# MAGIC %run ../Initializing_Notebook

# COMMAND ----------

# Importing ISBN library and creating UDF from it in order to be applied as a cleaning filter
from pyspark.sql.functions import udf
pip install isbnlib

from isbnlib import is_isbn10, is_isbn13




is_valid_isbn = udf(lambda x : is_isbn10(x) or is_isbn13(x), t.BooleanType())

# COMMAND ----------

# Creating a dataframe containing the ratings_bronze table

ratings_bronze = spark.table('ratings_bronze')

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

ratings_silver.write.format('delta').mode('overwrite').save(ratings_silver_path)
