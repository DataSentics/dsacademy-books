# Databricks notebook source
# MAGIC %run ../init_notebook

# COMMAND ----------

import booksutilities.bookslibrary as b
from pyspark.sql import functions as f

# COMMAND ----------

# Creating a dataframe containing the user_ratings table and books_silver table

user_ratings = spark.read.format('delta').load(f'{b.silver_files}/User_Ratings')
books = spark.read.format('delta').load(b.books_silver_path)

# COMMAND ----------

display(user_ratings)

# COMMAND ----------

# Finding the highest rater

highest_rater = (user_ratings
                 .join(books, 'ISBN', 'inner')
                 .sort(f.col('book_title'))
                 .drop('age', 'city', 'region', 'publisher')
                 .withColumnRenamed('avg(book_rating)', 'average_rating')
                 .filter(f.col('year_of_publication') >= 2000)
                 .groupBy('user_id', 'country').count()
                 .withColumnRenamed('count', 'number_of_reviews')
                 .sort(f.col('count').desc())
                 .limit(1))

# COMMAND ----------

# Displaying the highest rater

print('The user with most reviews given is from', f'{highest_rater.collect()[0][1]},',
      'with', highest_rater.collect()[0][2], 'total book reviews.')

# COMMAND ----------

# Saving highest_rater to path

highest_rater.write.format('delta').mode('overwrite').save(f'{b.gold_path}/highest_rater')