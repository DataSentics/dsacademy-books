# Databricks notebook source
# MAGIC %run /Repos/Book_Task/dsacademy-books/utilities/db_notebook

# COMMAND ----------

import utilities.utilities as u
import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

df_join_silver = spark.read.format("delta").load(u.joins_path)
display(df_join_silver.printSchema())

# COMMAND ----------

filtered_df = (df_join_silver
               .filter(F.col("BookRating") != 0)
               .filter("YearOfPublication is not NULL")
               .withColumn("YearsOfExistence", F.year(F.current_date()) - F.col("YearOfPublication")))

top_books_df = (filtered_df
                .groupBy("BookTitle", "YearOfPublication")
                .agg(F.avg("BookRating").alias("AverageRating"),
                     F.count("BookRating").alias("NumberOfReviews"),
                     (F.avg("BookRating") / (F.year(F.current_date()) - F.col("YearOfPublication")))
                .alias("AveragePerYear")))

result_df = (top_books_df
            .withColumn("CombinedScore",
                        average_rating_weight * F.col("AverageRating") 
                        + number_of_reviews_weight * F.col("NumberOfReviews")))


result_df = result_df.orderBy(F.col("CombinedScore").desc())
display(result_df)

# COMMAND ----------

average_age_per_book_df = (df_join_silver
                           .filter("Age is not NULL")
                           .groupBy("BookTitle").agg(F.avg("Age").alias("AverageAgeOfReaders")))

display(average_age_per_book_df)

# COMMAND ----------

average_age_per_book_df = average_age_per_book_df.withColumn(
    "AgeGroup",
    F.when((F.col("AverageAgeOfReaders") >= 1) & (F.col("AverageAgeOfReaders") <= 10), "1-10")
    .when((F.col("AverageAgeOfReaders") >= 11) & (F.col("AverageAgeOfReaders") <= 20), "11-20")
    .when((F.col("AverageAgeOfReaders") >= 21) & (F.col("AverageAgeOfReaders") <= 30), "21-30")
    .when((F.col("AverageAgeOfReaders") >= 31) & (F.col("AverageAgeOfReaders") <= 59), "31-59")
    .otherwise("60+")
)

display(average_age_per_book_df)

# COMMAND ----------

joined_df = (result_df
             .join(average_age_per_book_df, on="BookTitle", how="inner")
             .select("BookTitle", "AgeGroup", "CombinedScore").orderBy(F.col("CombinedScore").desc())
            )

display(joined_df)

# COMMAND ----------

best_books_df = (joined_df
                 .groupBy("AgeGroup").agg(F.max("CombinedScore").alias("MaxCombinedScore"),
                                          F.first("BookTitle").alias("BestBookTitle"))
                 .sort(F.col("AgeGroup"))
                 .drop("MaxCombinedScore"))
display(best_books_df)


# COMMAND ----------

best_books_df.write.format('delta').mode('overwrite').save(u.most_rated_per_agegroup)
