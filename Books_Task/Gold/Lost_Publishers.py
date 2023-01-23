# Databricks notebook source
# MAGIC %run ../init_notebook

# COMMAND ----------

import booksutilities.bookslibrary as b
from pyspark.sql import functions as f

# COMMAND ----------

# Assigning the needed tables to dataframes

books = spark.read.format('delta').load(f'{b.silver_files}/books_silver')

# COMMAND ----------

# Creating the dataframe containing the publishers
# that were lost in time, sorted by oldest publication

lost_publishers = (books
                   .groupBy(f.col('Publisher')).max('Year_of_publication')
                   .sort(f.col('max(Year_of_publication)'))
                   .withColumnRenamed('max(Year_of_publication)', 'Latest_publication'))

display(lost_publishers)

# COMMAND ----------

# Saving lost_publishers to path

lost_publishers.write.format('delta').mode('overwrite').save(f'{b.gold_path}/lost_publishers')
