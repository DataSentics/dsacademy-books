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
                          .drop('Book_Title', 'Book_Author'))

display(book_user_ratings)

# COMMAND ----------

author_per_country = (best_books_bayesian
                      .filter(f.col('Bayesian_score') > 20)
                      .join(temp_book_user_ratings, 'ISBN', 'inner')
                      .groupBy('country', 'Book_Author', 'Bayesian_score').count()
                      .withColumnRenamed('sum(Bayesian_score)', 'Bayesian_score'))

display(author_per_country)
author_per_country.printSchema()

# COMMAND ----------

top_authors_per_country = (author_per_country
                           .groupBy('country', 'Book_Author').agg({'Bayesian_score': 'sum'})
                           .sort(f.col('sum(Bayesian_score)').desc())
                           .groupBy('country').agg(f.collect_list('Book_Author'))
                           .withColumnRenamed('collect_list(Book_Author)', 'Top_Authors')
                           .withColumn('Top_Authors', f.slice('Top_Authors', 1, 10))
                           .withColumn('Top_Author', f.col('Top_Authors')[0])
                           .withColumn('country', f.initcap(f.col('country')))
                           .withColumn('country', f.when(f.col('country') == 'Usa',
                                                         'United States').otherwise(f.col('country'))))

display(top_authors_per_country)

# COMMAND ----------

top_authors_per_country.write.format('delta').mode('overwrite').save(f'{b.gold_path}/top_authors_per_country')
