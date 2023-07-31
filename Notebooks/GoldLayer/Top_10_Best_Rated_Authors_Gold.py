# Databricks notebook source
# MAGIC %run /Repos/Book_Task/dsacademy-books/utilities/db_notebook

# COMMAND ----------

import utilities.utilities as u
import pyspark.sql.functions as F

# COMMAND ----------

df_join_silver = spark.read.format("delta").load(u.joins_path)

# COMMAND ----------

display(df_join_silver)

# COMMAND ----------

def wilson_score(proportion_positive_ratings, total_ratings):
    wilson_score = ((proportion_positive_ratings + 1.96**2 / 
                     (2 * total_ratings) - 1.96 * 
                     F.sqrt((proportion_positive_ratings * 
                             (1 - proportion_positive_ratings) + 1.96**2 / 
                             (4 * total_ratings)) / total_ratings)) / 
                    (1 + 1.96**2 / total_ratings))
    return wilson_score

# wilson_score_udf = F.udf(wilson_score)

# COMMAND ----------

df_top_authors = (df_join_silver.filter(F.col("BookRating") != 0)
                  .withColumn("PositiveReviews", F.when(F.col("BookRating") >= 7, 1).otherwise(0))
                  .groupBy(F.initcap(F.col("BookAuthor")).alias("BookAuthor"))
                  .agg(F.count(F.col("BookRating")).alias("TotalReviews"),
                       F.round(F.avg(F.col("BookRating")), 2).alias("AverageRating"),
                       F.sum(F.col("PositiveReviews")).alias("PositiveReviews"))
                  .withColumn("ProportionPositive", F.round((F.col("PositiveReviews") / F.col("TotalReviews")), 2))
                  .withColumn("TrueAverage", F.round(wilson_score(F.col("ProportionPositive"),
                                                                  F.col("TotalReviews")) * 10, 2))
                  .drop("ProportionPositive")
                  .orderBy(["PositiveReviews", "TrueAverage"], ascending=[False, False])
                  .limit(10))

# display(df_top_authors.printSchema())
display(df_top_authors)

# COMMAND ----------

df_top_authors.write.format('delta').mode('overwrite').save(u.top_rated_authors)
