# Databricks notebook source
# MAGIC %run /Repos/Book_Task/dsacademy-books/utilities/db_notebook

# COMMAND ----------

import utilities.utilities as u
from pyspark.sql import functions as F

# COMMAND ----------

joined_df = spark.read.format('delta').load(u.joins_path)

# COMMAND ----------

books_df = (joined_df
            .withColumn('YearsOfExistence', F.year(F.current_date()) 
                        - F.col('YearOfPublication'))
            .select('ISBN', 'BookTitle', 'BookAuthor', 'YearsOfExistence')
            .distinct())

# COMMAND ----------

# average weight will specify how much importance to add to nr of reviews and averagePerYear columns

def get_ratings_gender(gender):
    return (joined_df
            .filter(F.col('bookRating') > 0)
            .filter(F.col('gender') == gender)
            .groupby('ISBN')
            .agg(F.round(F.avg("bookRating"), 2).alias("AvgRatings"),
                 F.count("bookRating").alias("NrRatings")))

def get_ratings_per_year(gender_df):
    return (gender_df
            .join(books_df, 'ISBN')
            .withColumn('AvgRatingsPerYear', F.round(F.col('AvgRatings') 
                                                     / F.col('YearsOfExistence'), 2))
            .withColumn("CombinedScore",
                        F.round(average_rating_weight * F.col("AvgRatingsPerYear") + number_of_reviews_weight * F.col("NrRatings"), 2))
            .sort('CombinedScore', ascending=False))

def get_top_10_books(gender_df, gender):
    return (gender_df
            .limit(10)
            .withColumn('Gender', F.lit(gender))
            .select('BookTitle', 'BookAuthor', 'Gender'))

# COMMAND ----------

# MAGIC %md # Explicit rating by female

# COMMAND ----------

ratings_female_df = get_ratings_gender('F')

# COMMAND ----------

ratings_per_year_female_df = get_ratings_per_year(ratings_female_df)

# COMMAND ----------

top_10_books_female = get_top_10_books(ratings_per_year_female_df, 'F')

# COMMAND ----------

# MAGIC %md # Explicit rating by male

# COMMAND ----------

ratings_male_df = get_ratings_gender('M')

# COMMAND ----------

ratings_per_year_male_df = get_ratings_per_year(ratings_male_df)

# COMMAND ----------

top_10_books_male = get_top_10_books(ratings_per_year_male_df, 'M')

# COMMAND ----------

# MAGIC %md # Union 10 top rated books per gender

# COMMAND ----------

top_10_books_gender = top_10_books_male.union(top_10_books_female)

# COMMAND ----------

top_10_books_gender.write.format('delta').mode('overwrite').save(u.top_rated_per_gender)
