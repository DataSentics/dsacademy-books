# Databricks notebook source
# MAGIC %run ../init_notebook

# COMMAND ----------

import booksutilities.bookslibrary as b
from pyspark.sql import functions as f

# COMMAND ----------

# Creating dataframe containing the User_Ratings table

user_ratings = spark.read.format('delta').load(f'{b.silver_files}/User_Ratings')

# COMMAND ----------

# Getting the average ratings per gender and age groups

average_per_group = (user_ratings
                     .filter(f.col('age').isNotNull())
                     .withColumn('age_group', f.concat((f.floor(f.col('age') / 10) * 10 + 1),
                                                       f.lit('-'),
                                                       f.floor(f.col('age') / 10) * 10 + 10))
                     .groupBy('gender', 'age_group').avg('book_rating')
                     .withColumnRenamed('avg(book_rating)', 'average_rating')
                     .withColumnRenamed('gender', 'gender')
                     .withColumn('average_rating', f.col('average_rating').cast('decimal(9, 2)'))
                     .withColumn('age_group2', f.lit((f.split(f.col("age_group"), "-")[0]).cast("int")))
                     .sort('gender', 'age_group2')
                     .drop('age_group2'))

display(average_per_group)

# COMMAND ----------

# Saving average_per_group to path

average_per_group.write.format('delta').mode('overwrite').save(f'{b.gold_path}/average_per_group')
