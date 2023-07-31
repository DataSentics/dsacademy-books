# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Author : Boboescu Denis
# MAGIC
# MAGIC # Using agg. functions to get the best books worldwide
# MAGIC

# COMMAND ----------

# MAGIC %run /Repos/Book_Task/dsacademy-books/utilities/db_notebook

# COMMAND ----------

import utilities.utilities as u
from pyspark.sql import functions as F

# COMMAND ----------

df_silver_join = spark.read.format('delta').load(u.joins_path)

# COMMAND ----------

# Checking the data

display(df_silver_join)

# COMMAND ----------

# Checking the schema
df_silver_join.printSchema()

# COMMAND ----------

# Obtaining the desired result ( best books worldwide )

df_top_10_rated_books_worldwide = (
    df_silver_join.filter(df_silver_join["BookRating"] != 0)
    .groupBy("ISBN", "BookTitle", "BookAuthor")
    .agg(F.count("BookRating").alias("RatingCount"),
        F.avg("BookRating").alias("AverageRating"))
    # .filter(F.col('AverageRating') > 8)
    .orderBy(F.col("RatingCount").desc(), F.col("AverageRating").desc())
    .limit(10)
)

# Average weight will specify how much importance to add to nr of reviews and review counts

number_of_reviews_weight = 0.1

# CombinedScore = sum of

df_best_books_by_score = df_top_10_rated_books_worldwide.withColumn("CombinedScore",
                                      average_rating_weight * F.col("AverageRating") +
                                      number_of_reviews_weight * F.col("RatingCount"))

top_books_worldwide = df_best_books_by_score.orderBy(F.col("CombinedScore").desc())

display(top_books_worldwide)

# COMMAND ----------

# Saving the resulted df to our blob storage

top_books_worldwide.write.format('delta').mode('overwrite').save(u.top_books_worldwide)
