# Databricks notebook source
# MAGIC %run ../init_notebook

# COMMAND ----------

import booksutilities.bookslibrary as b

# COMMAND ----------

# Importing books_silver and users_ratings in order to have
# data readily available for gold transformations

books_silver = spark.read.format('delta').load(b.books_silver_path)
user_ratings = spark.read.format('delta').load(f'{b.silver_files}/User_Ratings')

# COMMAND ----------

# Joining the two tables

books_user_ratings = (books_silver
                      .join(user_ratings, 'ISBN', 'inner')
                      .select('ISBN', 'user_ID', 'book_rating', 'age', 'book_title',
                              'book_author', 'country', 'year_of_publication', 'publisher'))

display(books_user_ratings)

# COMMAND ----------

# Saving books_user_ratings to path

books_user_ratings.write.format('delta').mode('overwrite').save(f'{b.silver_files}/Books_User_Ratings')
