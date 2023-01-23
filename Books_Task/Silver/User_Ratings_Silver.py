# Databricks notebook source
# MAGIC %run ../init_notebook

# COMMAND ----------

import booksutilities.bookslibrary as b

# COMMAND ----------

# Importing the users_pii table and the ratings table
# in order to create a new dataframe by joining the two

users_pii = spark.read.format('delta').load(b.users_pii_silver_path)
ratings = spark.read.format('delta').load(b.ratings_silver_path)

# COMMAND ----------

# Joining the two tables

user_ratings = (ratings
                .join(users_pii, 'User_ID', 'inner'))

display(user_ratings)

# COMMAND ----------

# Saving user_ratings to path

user_ratings.write.format('delta').mode('overwrite').save(f'{b.silver_files}/User_Ratings')
