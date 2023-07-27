# Databricks notebook source
# MAGIC %run /Repos/Book_Task/dsacademy-books/Notebooks/BronzeLayer/Utilities/db_notebook

# COMMAND ----------

import UtilitiesSilver.utilities as u
from pyspark.sql.functions import col, array_contains

# COMMAND ----------

ratings_bronze_df = spark.read.format('delta').load(u.ratings_bronze_path)
display(ratings_bronze_df)
row_count_bronze = ratings_bronze_df.count()
print(row_count_bronze)

# COMMAND ----------

ratings_cleaned_df = (ratings_bronze_df
                             .withColumnRenamed('User-ID', 'UserId')
                             .withColumnRenamed('Book-Rating', 'BookRating')
                             .withColumn('UserId', col('UserId').cast('long'))
                             .withColumn('BookRating', col('BookRating').cast('int'))
                             .dropDuplicates()
                             .dropna(subset = ("UserId", "ISBN"))
                             .drop('_rescued_data')
                            )
display(ratings_cleaned_df) 
row_count_cleaned = ratings_cleaned_df.count()
print(row_count_cleaned)                          

# COMMAND ----------

users_df = spark.table('users_bronze') 
users_df = users_df.withColumn('User-ID', col('User-ID').cast('long'))

ratings_users_valid_df = (ratings_cleaned_df
                          .join(users_df,ratings_cleaned_df['userId']==users_df['User-ID'])
                          .select(ratings_cleaned_df.UserId, ratings_cleaned_df.ISBN, ratings_cleaned_df.BookRating))

display(ratings_users_valid_df)
row_count_validated = ratings_users_valid_df.count()
print(row_count_validated)  

# COMMAND ----------


