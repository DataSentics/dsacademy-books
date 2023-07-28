# Databricks notebook source
# MAGIC %run /Repos/Book_Task/dsacademy-books/Notebooks/BronzeLayer/Utilities/db_notebook

# COMMAND ----------

import GoldUtilities.utilities as u
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

# average weight will specify how much importance to add to nr of reviews and averagePerYear columns
average_rating_weight = 4
number_of_reviews_weight = 0.1

result_df = (top_books_df
            .withColumn("CombinedScore", 
                        average_rating_weight * F.col("AverageRating") + number_of_reviews_weight * F.col("NumberOfReviews")))


result_df = result_df.orderBy(F.col("CombinedScore").desc())
display(result_df)

# COMMAND ----------

average_age_per_book_df = (df_join_silver
                           .filter("Age is not NULL")
                           .groupBy("BookTitle").agg(F.avg("Age").alias("AverageAgeOfReaders")))

display(average_age_per_book_df)

# COMMAND ----------

def get_age_group(average_age):
    if 1 <= average_age <= 10:
        return "1-10"
    elif 11 <= average_age <= 20:
        return "11-20"
    elif 21 <= average_age <= 30:
        return "21-30"
    elif 31 <= average_age <= 59:
        return "31-59"
    else:
        return "60+"

get_age_group_udf = udf(get_age_group, T.StringType())
spark.udf.register("get_age_group_udf", get_age_group, T.StringType())

average_age_per_book_df = average_age_per_book_df.withColumn("AgeGroup", get_age_group_udf(F.col("AverageAgeOfReaders")))
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
