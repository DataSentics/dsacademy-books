# Databricks notebook source
# MAGIC %run ../initial_notebook

# COMMAND ----------

# I WAS NOT ABLE TO MAKE IT WORK WITH pyspark.dataframe

# from pyspark.sql.functions import udf

# udf_func = udf(fixing_author_column)

# def fixing_author_column(column_name):
#     for col in column_name:
#         new_name= ''
#         for i in range(len(col)):
#             if col[i] == '.' and col[i+1] !=' ' :
#                 new_name = new_name + '. '
#             else:
#                 new_name = new_name + col[i]
#     return new_name

# COMMAND ----------

from pyspark.sql.functions import col, trim, when, upper

(spark.read.table('books_bronze')
 .withColumn('Book-Title', trim(upper(col('Book-Title'))))
 .withColumn('Book-Author', upper(col('Book-Author')))
 .withColumn('Publisher', trim(upper(col('Publisher'))))
 .withColumn('Year-Of-Publication', col('Year-Of-Publication').cast('integer'))
 .withColumn('Year-Of-Publication', when((col('Year-Of-Publication') < 1376) |
                                         (col('Year-Of-Publication') > 2021), None)
             .otherwise(col('Year-Of-Publication')))
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("path", books_cleansed_path)
 .saveAsTable("books_silver"))
