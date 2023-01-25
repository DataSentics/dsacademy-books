# Databricks notebook source
# MAGIC %run ../init_notebook

# COMMAND ----------

import booksutilities.bookslibrary as b
from pyspark.sql import functions as f

# COMMAND ----------

book_user_ratings = spark.read.format('delta').load(f'{b.silver_files}/Books_User_Ratings')
best_authors_bayesian = spark.read.format('delta').load(f'{b.gold_path}/best_authors_bayesian')
best_books_bayesian = spark.read.format('delta').load(f'{b.gold_path}/best_books_bayesian')

# COMMAND ----------

display(best_books_bayesian)

# COMMAND ----------

temp_book_user_ratings = (book_user_ratings
                          .drop('book_title', 'book_author'))

display(temp_book_user_ratings)

# COMMAND ----------

author_per_country = (best_books_bayesian
                      .filter(f.col('bayesian_score') > 20)
                      .join(temp_book_user_ratings, 'ISBN', 'inner')
                      .groupBy('country', 'book_author', 'bayesian_score').count()
                      .withColumnRenamed('sum(bayesian_score)', 'bayesian_score'))

display(author_per_country)
author_per_country.printSchema()

# COMMAND ----------

top_authors_per_country = (author_per_country
                           .groupBy('country', 'book_author').agg({'bayesian_score': 'sum'})
                           .sort(f.col('sum(bayesian_score)').desc())
                           .groupBy('country').agg(f.collect_list('book_author'))
                           .withColumnRenamed('collect_list(book_author)', 'top_authors')
                           .withColumn('top_authors', f.slice('top_authors', 1, 10))
                           .withColumn('top_author', f.col('top_authors')[0])
                           .withColumn('country', f.initcap(f.col('country')))
                           .withColumn('country', f.when(f.col('country') == 'Usa',
                                                         'United States').otherwise(f.col('country'))))

display(top_authors_per_country)

# COMMAND ----------

top_authors_per_country.write.format('delta').mode('overwrite').save(f'{b.gold_path}/top_authors_per_country')
