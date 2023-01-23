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
                .withColumnRenamed('Book-Title', 'Book_Title')
                .withColumnRenamed('Book-Author', 'Book_Author')
                .withColumnRenamed('Year-Of-Publication', 'Year_of_publication')
                .withColumn('ISBN', f.regexp_replace(f.col('ISBN'), '[^0-9X]', ''))
                .filter(is_valid_isbn(f.col("ISBN")))
                .withColumn('Book_Author', f.initcap(f.trim(f.regexp_replace(f.col('Book_Author'),
                                                                             (r'(?<=[A-Za-z])\.(?=[A-Za-z])'), '. '))))
                .withColumn('Book_Author', f.regexp_replace(f.col('Book_Author'), '&amp;', '&'))
                .withColumn('Book_Title', f.regexp_replace(f.col('Book_Title'), '&amp;', '&'))
                .withColumn('Publisher', f.regexp_replace(f.col('Publisher'), '&amp;', '&'))
                .withColumn(('Year_of_publication'), f.col('Year_of_publication').cast('integer'))
                .withColumn("Year_of_publication",
                            f.when((f.col("Year_of_publication") == 0) | (f.col("Year_of_publication") > 2023), None)
                            .otherwise(f.col("Year_of_publication")))
                .drop('Image-URL-S', 'Image-URL-M', 'Image-URL-L', '_rescued_data'))

# COMMAND ----------

# Displaying books_silver

display(books_silver
        .filter(f.col('Year_of_publication') > 2008))
books_silver.printSchema()

# COMMAND ----------

# Saving books_silver to path

books_silver.write.format('delta').mode('overwrite').save(b.books_silver_path)
