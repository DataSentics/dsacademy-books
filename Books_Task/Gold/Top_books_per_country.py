# Databricks notebook source
# MAGIC %run ../init_notebook

# COMMAND ----------

import booksutilities.bookslibrary as b
from pyspark.sql import functions as f

# COMMAND ----------

book_user_ratings = spark.read.format('delta').load(f'{b.silver_files}/Books_User_Ratings')
best_books_bayesian = spark.read.format('delta').load(f'{b.gold_path}/best_books_bayesian')

# COMMAND ----------

books_per_country = (best_books_bayesian
                     .join(book_user_ratings, 'ISBN', 'inner')
                     .filter(f.col('bayesian_score') > 20)
                     .groupBy('country', 'ISBN').count())

display(books_per_country)

# COMMAND ----------

top_books_per_country = (books_per_country
                         .groupBy('country', 'ISBN').agg({'count': 'sum'})
                         .sort(f.col('sum(count)').desc())
                         .groupBy('country').agg(f.collect_list('ISBN'))
                         .withColumnRenamed('country', 'country_temp')
                         .withColumnRenamed('collect_list(ISBN)', 'top_books')
                         .withColumn('top_books', f.slice('top_books', 1, 10))
                         .withColumn('ISBN', f.col('top_books')[0])
                         .withColumn('country_temp', f.initcap(f.col('country_temp')))
                         .withColumn('country_temp', f.when(f.col('country_temp') == 'Usa',
                                                            'United States').otherwise(f.col('country_temp')))
                         .join(book_user_ratings, 'ISBN', 'inner')
                         .select('country_temp', 'top_books', 'ISBN', 'book_title', 'book_author')
                         .withColumnRenamed('country_temp', 'country')
                         .withColumnRenamed('ISBN', 'top_book')
                         .groupBy('country', 'top_books', 'top_book', 'book_title', 'book_author').count()
                         .drop('count')
                         .sort('country'))

display(top_books_per_country)

# COMMAND ----------

top_books_per_country.write.format('delta').mode('overwrite').save(f'{b.gold_path}/top_books_per_country')
