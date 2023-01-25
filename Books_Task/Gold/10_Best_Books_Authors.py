# Databricks notebook source
# MAGIC %run ../init_notebook

# COMMAND ----------

import booksutilities.bookslibrary as b
from pyspark.sql import functions as f
from pyspark.sql import types as t
import math
import scipy.stats as st

# COMMAND ----------

# Assigning the needed tables to dataframes

book_user_ratings = spark.read.format('delta').load(f'{b.silver_files}/Books_User_Ratings')
books = spark.read.format('delta').load(f'{b.silver_files}/books_silver')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting up 10 best authors with Wilson Confidence Interval method

# COMMAND ----------

# Wilson Confidence Interval Function

def wilson_lower_bound(pos, n, confidence=0.95):
    """
    Function to provide lower bound of wilson score
    :param pos: No of positive ratings
    :param n: Total number of ratings
    :param confidence: Confidence interval, by default is 95 %
    :return: Wilson Lower bound score
    """
    if n == 0:
        return 0
    z = st.norm.ppf(1 - (1 - confidence) / 2)
    phat = 1.0 * pos / n
    x = (phat + z * z / (2 * n) - z * math.sqrt((phat * (1 - phat) + z * z / (4 * n)) / n)) / (1 + z * z / n)
    return float(x)

spark.udf.register('wilson_lower_bound_UDF', wilson_lower_bound, t.DoubleType())

udf_wilson = f.udf(wilson_lower_bound, t.DoubleType())

# COMMAND ----------

# Getting the number of positive and negative reviews per book
# Applying the Wilson Confidence Interval
# Finding the best books through this method

# This method returns generally considered best books, being weighted
# on the number of positive reviews compared to the total number of reviews

best_books_wilson = (book_user_ratings
                     .withColumn('positive_review', f.when(f.col('book_rating') >= 8, 1).otherwise(0))
                     .withColumn('negative_review', f.when(f.col('book_rating') < 8, 1).otherwise(0))
                     .groupBy('ISBN').agg({'ISBN': 'count', 'book_rating': 'avg',
                                           'positive_review': 'sum', 'negative_review': 'sum'})
                     .withColumnRenamed('avg(book_rating)', 'average_rating')
                     .withColumn('average_rating', f.col('average_rating').cast('decimal(9, 2)'))
                     .withColumnRenamed('count(ISBN)', 'total_reviews')
                     .withColumnRenamed('sum(negative_review)', 'negative_reviews')
                     .withColumnRenamed('sum(positive_review)', 'positive_reviews')
                     .withColumn('wilson_confidence', udf_wilson(f.col('positive_reviews'), f.col('total_reviews')))
                     .withColumn('wilson_confidence', f.col('wilson_confidence').cast('decimal(9, 4)'))
                     .join(books, 'ISBN', 'inner')
                     .select('ISBN', 'wilson_confidence', 'average_rating', 'total_reviews',
                             'book_title', 'book_author', 'year_of_publication', 'publisher')
                     .sort(f.col('wilson_confidence').desc()))

display(best_books_wilson)
best_books_wilson.printSchema()

# COMMAND ----------

# Getting the total book number to weight the average
# Wilson score on number of books written by each author

book_number = (best_books_wilson
               .select('book_author', 'ISBN')
               .groupBy('book_author').count())

# COMMAND ----------

# Obtaining the final score and best authors sorted

best_authors_wilson = (best_books_wilson
                       .groupBy('book_author').agg(f.avg('wilson_confidence'))
                       .join(book_number, 'book_author', 'inner')
                       .withColumnRenamed('count', 'books_written')
                       .withColumnRenamed('avg(wilson_confidence)', 'wilson_score')
                       .withColumn('final_score', f.col('wilson_score') * f.col('books_written'))
                       .drop('wilson_score')
                       .sort(f.col('final_score').desc()))


display(best_authors_wilson)
best_authors_wilson.count()

# COMMAND ----------

# Saving best_books_wilson and best_authors_wilson to path

best_books_wilson.write.format('delta').mode('overwrite').save(f'{b.gold_path}/best_books_wilson')
best_authors_wilson.write.format('delta').mode('overwrite').save(f'{b.gold_path}/best_authors_wilson')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting up 10 best authors with Bayesian Credible Interval method

# COMMAND ----------

# Bayesian Credibile Interval Function

def bayesian_rating_products(n, confidence=0.95):
    """
    Function to calculate wilson score for N star rating system.
    :param n: Array having count of star ratings where ith index
    represent the votes for that category i.e. [3, 5, 6, 7, 10]
    here, there are 3 votes for 1-star rating,
    similarly 5 votes for 2-star rating.
    :param confidence: Confidence interval
    :return: Score
    """
    if sum(n) == 0:
        return 0
    K = len(n)
    z = st.norm.ppf(1 - (1 - confidence) / 2)
    N = sum(n)
    first_part = 0.0
    second_part = 0.0
    for k, n_k in enumerate(n):
        first_part += (k + 1) * (n[k] + 1) / (N + K)
        second_part += (k + 1) * (k + 1) * (n[k] + 1) / (N + K)
    score = first_part - z * math.sqrt((second_part - first_part * first_part) / (N + K + 1))
    return float(score)

spark.udf.register('bayesian_udf', bayesian_rating_products, t.DoubleType())

udf_bayesian = f.udf(bayesian_rating_products, t.DoubleType())

# COMMAND ----------

# Applying the Bayesian Credible Interval
# Finding the best books through this method

# This method returns the 'best popular books', being heavily weighted
# on number of reviews

best_books_bayesian = (book_user_ratings
                       .groupBy('ISBN', 'book_title', 'book_author', 'year_of_publication', 'publisher')
                       .agg(f.collect_list('book_rating'), f.avg('book_rating'))
                       .withColumnRenamed('collect_list(book_rating)', 'ratings')
                       .withColumnRenamed('avg(book_rating)', 'average_rating')
                       .filter(f.col('average_rating') >= 8)
                       .withColumn('bayesian_score', udf_bayesian(f.col('ratings')))
                       .withColumn('bayesian_score', f.col('bayesian_score').cast('decimal(9, 4)'))
                       .withColumn('average_rating', f.col('average_rating').cast('decimal(9, 2)'))
                       .sort(f.col('bayesian_score').desc(), f.col('average_rating').desc())
                       .select('ISBN', 'bayesian_score', 'average_rating', 'book_title',
                               'book_author', 'year_of_publication', 'publisher')
                       .drop('Ratings'))

display(best_books_bayesian)

# COMMAND ----------

# Obtaining the final score and best authors sorted

best_authors_bayesian = (best_books_bayesian
                         .groupBy('book_author').agg(f.avg('bayesian_score'))
                         .join(book_number, 'book_author', 'inner')
                         .withColumnRenamed('count', 'books_written')
                         .withColumnRenamed('avg(bayesian_score)', 'bayesian_score')
                         .withColumn('final_score', f.col('bayesian_score') * f.col('books_written'))
                         .drop('bayesian_score')
                         .withColumnRenamed('final_score', 'weighted_score')
                         .sort(f.col('weighted_score').desc()))

display(best_authors_bayesian)

# COMMAND ----------

best_authors_bayesian_test = (best_books_bayesian
                              .groupBy('book_author').agg(f.sum('bayesian_score'))
                              .join(book_number, 'book_author', 'inner')
                              .withColumnRenamed('count', 'books_written')
                              .withColumnRenamed('sum(bayesian_score)', 'bayesian_score')
                              .sort(f.col('bayesian_score').desc()))

display(best_authors_bayesian_test)

# COMMAND ----------

# Saving best_books_bayesian and best_authos_wilson to path

best_books_bayesian.write.format('delta').mode('overwrite').save(f'{b.gold_path}/best_books_bayesian')
best_authors_bayesian.write.format('delta').mode('overwrite').save(f'{b.gold_path}/best_authors_bayesian')
