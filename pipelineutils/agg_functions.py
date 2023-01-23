from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("books_pipeline").getOrCreate()

from pyspark.sql import functions as f

def best_rated_popular_books(min_no_of_ratings=200):
    """The function takes one argument, a minimum number of ratings/book for a customizable ranking, and returns a dataframe with the results"""
    
    resulting_df = (spark
                    .table("books_silver")
                    .join(spark.table("book_ratings_silver"), "ISBN", "inner")
                    .groupBy("BOOK_TITLE", "ISBN")
                    .agg((f.count("ISBN").alias("NO_OF_RATINGS")), (f.mean(f.col("BOOK_RATING")).alias("AVG_RATING")))
                    .sort(f.col("AVG_RATING").desc())
                    .where(f"NO_OF_RATINGS > {min_no_of_ratings}")
                    .limit(10)
                    )
    return resulting_df

def best_rated_popular_books_after(from_year=2000, min_no_of_ratings=200):
    """The function takes 2 arguments to specify the desired year to filter by and a minimum number of ratings/book for a customizable ranking, all as integers, and returns a dataframe with the results"""
    
    resulting_df = (spark
                    .table("books_silver")
                    .join(spark.table("book_ratings_silver"), "ISBN", "inner")
                    .filter((f.col("YEAR_OF_PUBLICATION") >= from_year))
                    .groupBy("BOOK_TITLE", "ISBN")
                    .agg((f.count("ISBN").alias("NO_OF_RATINGS")), (f.mean(f.col("BOOK_RATING")).alias("AVG_RATING")))
                    .sort(f.col("AVG_RATING").desc())
                    .where(f"NO_OF_RATINGS > {min_no_of_ratings}")
                    .limit(10)
                    )
    return resulting_df

def best_10_books_by_sex_age_country(sex, age_interval, country, min_no_of_ratings):
    """The function takes 4 arguments, 'sex' as a string representing the gender M or F of the reviewer, 'country' as a string representing the reviewers country a list of two integers as values for 'age_interval' and a minimum number of ratings/book, all act as filters, the function returns a dataframe with the results"""
    
    resulting_df = (spark
                    .table("books_silver")
                    .join(spark.table("book_ratings_silver"), "ISBN", "inner")
                    .join(spark.table("users_with_pii_silver"), "USER_ID", "inner")
                    .filter((f.col("GENDER") == sex) & (f.col("AGE") >= age_interval[0]) & (f.col("AGE") <= age_interval[1]) & (f.col("COUNTRY") == country))
                    .groupBy("BOOK_TITLE", "ISBN")
                    .agg((f.count("ISBN").alias("NO_OF_RATINGS")), (f.mean(f.col("BOOK_RATING")).alias("AVG_RATING")),
                        (f.max(f.col("GENDER")).alias("REVIEWERS_GENDER")),
                         (f.max(f.col("COUNTRY")).alias("REVIEWERS_COUNTRY"))
                        )
                    .withColumn("REVIEWERS_AGE_GROUP", f.lit(f"{age_interval[0]} - {age_interval[1]}"))
                    .sort(f.col("AVG_RATING").desc())
                    .where(f"NO_OF_RATINGS > {min_no_of_ratings}")
                    .limit(10)
                    )
    return resulting_df