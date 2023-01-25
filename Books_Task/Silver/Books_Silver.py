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

# Creating a dataframe containing the books_bronze table

books_bronze = spark.read.format('delta').load(b.books_silver_path)

# COMMAND ----------

# Displaying books_bronze

display(books_bronze)
books_bronze.count()

# COMMAND ----------

# Cleaning books_bronze

books_silver = (books_bronze
                .withColumnRenamed('Book-Title', 'book_title')
                .withColumnRenamed('Book-Author', 'book_author')
                .withColumnRenamed('Year-Of-Publication', 'year_of_publication')
                .withColumn('ISBN', f.regexp_replace(f.col('ISBN'), '[^0-9X]', ''))
                .filter(is_valid_isbn(f.col("ISBN")))
                .withColumn('book_author', f.initcap(f.trim(f.regexp_replace(f.col('book_author'),
                                                                             (r'(?<=[A-Za-z])\.(?=[A-Za-z])'), '. '))))
                .withColumn('book_author', f.regexp_replace(f.col('book_author'), '&amp;', '&'))
                .withColumn('book_title', f.regexp_replace(f.col('book_title'), '&amp;', '&'))
                .withColumn('publisher', f.regexp_replace(f.col('publisher'), '&amp;', '&'))
                .withColumn(('year_of_publication'), f.col('year_of_publication').cast('integer'))
                .withColumn("year_of_publication",
                            f.when((f.col("year_of_publication") == 0) | (f.col("year_of_publication") > 2023), None)
                            .otherwise(f.col("year_of_publication")))
                .drop('Image-URL-S', 'Image-URL-M', 'Image-URL-L', '_rescued_data'))

# COMMAND ----------

# Displaying books_silver

display(books_silver
        .filter(f.col('year_of_publication') > 2008))
books_silver.printSchema()

# COMMAND ----------

# Saving books_silver to path

books_silver.write.format('delta').mode('overwrite').save(b.books_silver_path)
