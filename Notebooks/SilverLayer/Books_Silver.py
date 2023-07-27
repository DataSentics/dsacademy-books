# Databricks notebook source
# MAGIC %run /Repos/Book_Task/dsacademy-books/Notebooks/BronzeLayer/Utilities/db_notebook

# COMMAND ----------

import UtilitiesSilver.utilities as U

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

books_bronze = spark.read.format('delta').load(u.books_bronze_path)

# COMMAND ----------

books_bronze.printSchema()

# COMMAND ----------

display(books_bronze)

# COMMAND ----------

books_silver = (books_bronze
                .withColumnRenamed('Book-Title', 'BookTitle')
                .withColumnRenamed('Book-Author','BookAuthor')
                .withColumnRenamed('Year-Of-Publication','YearOfPublication')
                .withColumn('Year',F.col('YearOfPublication').cast('int'))
                .withColumn('BookAuthor', F.regexp_replace( F.col('BookAuthor'), U.regex_pattern,' '))
                .withColumn('BookTitle', F.regexp_replace(F.col('BookTitle'), '&amp','&') )
                .withColumn('Publisher', F.regexp_replace(F.col('Publisher'),'&amp','&'))
                .drop('Image-URL-S', 'Image-URL-M', 'Image-URL-L', 'Year', '_rescued_data')
                )

# COMMAND ----------

display(books_silver)

# COMMAND ----------



# COMMAND ----------

books_silver.printSchema()

# COMMAND ----------

books_silver.write.format('delta').mode('overwrite').save(u.books_silver_path)

# COMMAND ----------


