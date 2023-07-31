# Databricks notebook source
# MAGIC %run /Repos/Book_Task/dsacademy-books/utilities/db_notebook

# COMMAND ----------

import utilities.utilities as u
from pyspark.sql import functions as F

# COMMAND ----------

ratings_bronze_df = spark.read.format('delta').load(u.ratings_bronze_path)
row_count_bronze = ratings_bronze_df.count()
print(row_count_bronze)

# COMMAND ----------

ratings_cleaned_df = (ratings_bronze_df
                      .withColumnRenamed('User-ID', 'UserId')
                      .withColumnRenamed('Book-Rating', 'BookRating')
                      .withColumn('UserId', F.col('UserId').cast('long'))
                      .withColumn('BookRating', F.col('BookRating').cast('int'))
                      .dropDuplicates()
                      .dropna(subset=("UserId", "ISBN"))
                      .drop('_rescued_data'))
row_count_cleaned = ratings_cleaned_df.count()
print(row_count_cleaned)

# COMMAND ----------

users_df = spark.table('users_bronze')
users_df = users_df.withColumn('User-ID', F.col('User-ID').cast('long'))
ratings_users_valid_df = (ratings_cleaned_df
                          .join(users_df, ratings_cleaned_df['userId'] == users_df['User-ID'])
                          .select(ratings_cleaned_df.UserId, ratings_cleaned_df.ISBN, ratings_cleaned_df.BookRating))
row_count_validated = ratings_users_valid_df.count()
print(row_count_validated)

# COMMAND ----------

ratings_users_valid_df.write.format('delta').mode('overwrite').save(u.ratings_silver_path)
