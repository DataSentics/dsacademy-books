# Databricks notebook source
import pyspark.sql.functions as f
import mypackage.mymodule as m

# COMMAND ----------

# MAGIC %run ../init_setup

# COMMAND ----------

silver_df_books = (spark
                   .table('bronze_books')
                   .withColumn('Book-Title', f.trim(f.initcap(f.lower(f.col('Book-Title')))))
                   .withColumn('Book-Author', f.trim(f.initcap(f.lower(f.col('Book-Author')))))
                   .withColumn('Publisher', f.trim(f.initcap(f.lower(f.col('Publisher')))))
                   .withColumn('Year-Of-Publication', f.col('Year-Of-Publication').cast('integer'))
                   .withColumn('Year-Of-Publication', f.when((f.col('Year-Of-Publication') < 1376) |
                               (f.col('Year-Of-Publication') > 2021), None)
                               .otherwise(f.col('Year-Of-Publication'))))

# COMMAND ----------

m.write_silver(silver_df_books, m.silver_books_path, 'silver_books')
