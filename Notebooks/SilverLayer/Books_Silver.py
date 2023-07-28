# Databricks notebook source
# MAGIC %run /Repos/Book_Task/dsacademy-books/Notebooks/BronzeLayer/Utilities/db_notebook

# COMMAND ----------

import UtilitiesSilver.utilities as U

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

# loading the data from the specified blob storage path

books_bronze = spark.read.format('delta').load(U.books_bronze_path)

# COMMAND ----------

#Checking the schema
books_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Performing several transformations on the DataFrame books_bronze to create a new DataFrame called books_silver. The transformations include renaming columns, converting the "Year-Of-Publication" column to a date, handling invalid year values, cleaning strings using regular expressions, and dropping unnecessary columns.

# COMMAND ----------

books_silver = (books_bronze
                .withColumnRenamed('Book-Title', 'BookTitle')
                .withColumnRenamed('Book-Author', 'BookAuthor')
                .withColumnRenamed('Year-Of-Publication', 'YearOfPublication')
                .withColumn('YearOfPublication', F.to_date(F.col("YearOfPublication"), "yyyy"))
                .withColumn('YearOfPublication', 
                            F.when((F.col('YearOfPublication') == F.lit('0000')) | 
                                 (F.col('YearOfPublication') > F.current_date()), None)
                            .otherwise(F.year(F.col('YearOfPublication'))))
                .withColumn('BookAuthor', F.regexp_replace(F.col('BookAuthor'), U.regex_pattern, ' '))
                .withColumn('BookTitle', F.regexp_replace(F.col('BookTitle'), '&amp', '&'))
                .withColumn('Publisher', F.regexp_replace(F.col('Publisher'), '&amp', '&'))
                .drop('Image-URL-S', 'Image-URL-M', 'Image-URL-L', '_rescued_data')
               )

# COMMAND ----------

display(books_silver)

# COMMAND ----------

books_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Writing the DataFrame books_silver to a Delta Lake table at the specified location U.books_silver_path

# COMMAND ----------

books_silver.write.format('delta').mode('overwrite').save(U.books_silver_path)

# COMMAND ----------


