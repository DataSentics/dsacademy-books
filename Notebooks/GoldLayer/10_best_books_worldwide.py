# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Author : Boboescu Denis
# MAGIC
# MAGIC # Using agg. functions to get the best books worldwide
# MAGIC

# COMMAND ----------

# MAGIC %run /Repos/Book_Task/dsacademy-books/Notebooks/BronzeLayer/Utilities/db_notebook

# COMMAND ----------

import GoldUtilities.utilities as u
from pyspark.sql import functions as F

# COMMAND ----------

df_silver_join = spark.read.format('delta').load(u.joins_path)

# COMMAND ----------

#Checking the data

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
      #.filter(F.col('AverageRating') > 8)
      .orderBy(F.col("RatingCount").desc(), F.col("AverageRating").desc())
      .limit(10)
)


# average weight will specify how much importance to add to nr of reviews and averagePerYear columns

average_rating_weight = 4

number_of_reviews_weight = 0.1

 

# CombinedScore = sum of

result_df = df_top_10_rated_books_worldwide.withColumn("CombinedScore",

                                 average_rating_weight * F.col("AverageRating") +

                                 number_of_reviews_weight * F.col("RatingCount"))

result_df = result_df.orderBy(F.col("CombinedScore").desc())
display(result_df)

# COMMAND ----------

# Saving the resulted df to our blob storage

df_top_10_rated_books_worldwide.write.format('delta').mode('overwrite').save(u.top_books_worldwide)
