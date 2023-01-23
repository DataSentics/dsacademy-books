# Databricks notebook source
# MAGIC %run ../initial_notebook

# COMMAND ----------

# I WAS NOT ABLE TO MAKE IT WORK WITH pyspark.dataframe

# from pyspark.sql.functions import udf

# def fixing_author_column(column_name):
#     for col in column_name:
#         new_name= ''
#         for i in range(len(col)):
#             if col[i] == '.' and col[i+1] !=' ' :
#                 new_name = new_name + '. '
#             else:
#                 new_name = new_name + col[i]
#     return new_name

# udf_func = udf(fixing_author_column)

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper

df = spark.read.format('delta').load(
    f'{path_to_cleansed_storage}/books_silver')


df_final = (df.withColumn('Book-Title', trim(upper(col('Book-Title'))))
            #  .withColumn('Book-Author', fixing_author_column(col("Book-Author")))
            .withColumn('Publisher', trim(upper(col('Publisher'))))
            .withColumn('Year-Of-Publication', col('Year-Of-Publication').cast('integer'))
            .where((col('Year-Of-Publication') < 2022) & (col('Year-Of-Publication') > 1806) & 
                  (col('Year-Of-Publication') != 0))
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("path", books_cleansed_path)
            .saveAsTable("books_silver"))
