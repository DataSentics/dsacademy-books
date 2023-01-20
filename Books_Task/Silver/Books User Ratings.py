# Databricks notebook source
# MAGIC %run ../Initializing_Notebook

# COMMAND ----------

# Importing books_silver and users_ratings in order to have
# data readily available for gold transformations

books_silver = spark.read.format('delta').load(books_silver_path)
user_ratings = spark.read.format('delta').load(f'{silver_files}/User_Ratings')

# COMMAND ----------

# Joining the two tables

books_user_ratings = (books_silver
                      .join(user_ratings, 'ISBN', 'inner')
                      .select('ISBN', 'User_ID', 'Book_Rating', 'Age', 'Book_Title',
                              'Book_Author', 'Country', 'Year_of_publication', 'Publisher'))

display(books_user_ratings)

# COMMAND ----------

# Saving books_user_ratings to path

books_user_ratings.write.format('delta').mode('overwrite').save(f'{silver_files}/Books_User_Ratings')