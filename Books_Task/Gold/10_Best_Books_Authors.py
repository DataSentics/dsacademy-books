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
                     .withColumn('Positive_review', f.when(f.col('Book_Rating') >= 8, 1).otherwise(0))
                     .withColumn('Negative_review', f.when(f.col('Book_Rating') < 8, 1).otherwise(0))
                     .groupBy('ISBN').agg({'ISBN': 'count', 'Book_Rating': 'avg',
                                           'Positive_review': 'sum', 'Negative_review': 'sum'})
                     .withColumnRenamed('avg(Book_Rating)', 'Average_rating')
                     .withColumn('Average_rating', f.col('Average_rating').cast('decimal(9, 2)'))
                     .withColumnRenamed('count(ISBN)', 'Total_reviews')
                     .withColumnRenamed('sum(Negative_review)', 'Negative_reviews')
                     .withColumnRenamed('sum(Positive_review)', 'Positive_reviews')
                     .withColumn('Wilson_confidence', udf_wilson(f.col('Positive_reviews'), f.col('Total_reviews')))
                     .withColumn('Wilson_confidence', f.col('Wilson_confidence').cast('decimal(9, 4)'))
                     .join(books, 'ISBN', 'inner')
                     .select('ISBN', 'Wilson_confidence', 'Average_rating', 'Total_reviews',
                             'Book_Title', 'Book_Author', 'Year_of_publication', 'Publisher')
                     .sort(f.col('Wilson_confidence').desc()))

display(best_books_wilson)
best_books_wilson.printSchema()

# COMMAND ----------

# Getting the total book number to weight the average
# Wilson score on number of books written by each author

book_number = (best_books_wilson
               .select('Book_Author', 'ISBN')
               .groupBy('Book_Author').count())

# COMMAND ----------

# Obtaining the final score and best authors sorted

best_authors_wilson = (best_books_wilson
                       .groupBy('Book_Author').agg(f.avg('Wilson_confidence'))
                       .join(book_number, 'Book_Author', 'inner')
                       .withColumnRenamed('count', 'Books_written')
                       .withColumnRenamed('avg(Wilson_confidence)', 'Wilson_score')
                       .withColumn('Final_score', f.col('Wilson_score') * f.col('Books_written'))
                       .drop('Wilson_score')
                       .sort(f.col('Final_score').desc()))


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
                       .groupBy('ISBN', 'Book_Title', 'Book_Author', 'Year_of_publication', 'Publisher')
                       .agg(f.collect_list('Book_Rating'), f.avg('Book_Rating'))
                       .withColumnRenamed('collect_list(Book_Rating)', 'Ratings')
                       .withColumnRenamed('avg(Book_Rating)', 'Average_rating')
                       .filter(f.col('Average_rating') >= 8)
                       .withColumn('Bayesian_score', udf_bayesian(f.col('Ratings')))
                       .withColumn('Bayesian_score', f.col('Bayesian_score').cast('decimal(9, 4)'))
                       .withColumn('Average_rating', f.col('Average_rating').cast('decimal(9, 2)'))
                       .sort(f.col('Bayesian_score').desc(), f.col('Average_rating').desc())
                       .select('ISBN', 'Bayesian_score', 'Average_rating', 'Book_Title',
                               'Book_Author', 'Year_of_publication', 'Publisher')
                       .drop('Ratings'))

display(best_books_bayesian)

# COMMAND ----------

# Obtaining the final score and best authors sorted

best_authors_bayesian = (best_books_bayesian
                         .groupBy('Book_Author').agg(f.avg('Bayesian_score'))
                         .join(book_number, 'Book_Author', 'inner')
                         .withColumnRenamed('count', 'Books_written')
                         .withColumnRenamed('avg(Bayesian_score)', 'Bayesian_score')
                         .withColumn('Final_score', f.col('Bayesian_score') * f.col('Books_written'))
                         .drop('Bayesian_score')
                         .withColumnRenamed('Final_score', 'Weighted_score')
                         .sort(f.col('Weighted_score').desc()))

display(best_authors_bayesian)

# COMMAND ----------

best_authors_bayesian_test = (best_books_bayesian
                              .groupBy('Book_Author').agg(f.sum('Bayesian_score'))
                              .join(book_number, 'Book_Author', 'inner')
                              .withColumnRenamed('count', 'Books_written')
                              .withColumnRenamed('sum(Bayesian_score)', 'Bayesian_score')
                              .sort(f.col('Bayesian_score').desc()))

display(best_authors_bayesian_test)

# COMMAND ----------

# Saving best_books_bayesian and best_authos_wilson to path

best_books_bayesian.write.format('delta').mode('overwrite').save(f'{b.gold_path}/best_books_bayesian')
best_authors_bayesian.write.format('delta').mode('overwrite').save(f'{b.gold_path}/best_authors_bayesian')
