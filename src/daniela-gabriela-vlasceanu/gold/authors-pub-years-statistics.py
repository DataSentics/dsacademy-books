# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

# CREATING THE COLUMNS FOR THE NEXT DF, WITH NUM_OF_MIN_RATINGS AND AVG_RATING_IN_TOTAL
year_publisher_ratings = spark.table("authors_pub_years")
intermediar_df = 
( year_publisher_ratings
 .groupBy("Year-Of-Publication", "Publisher")
 .agg(
    f.avg("Number-of-ratings").cast("int").alias("min-votes-required"),
    f.avg("Rating-Average").alias("Avg-note"),
 )
)

# COMMAND ----------

# Using the IMDB rating formula to calculate,(num_ratings * avg_rating_by_year_publisher + min_num_ratings * avg_ratings_total)/(num_ratings + min_num_ratings)
df_joined = year_publisher_ratings.join(
    intermediar_df, ["Year-Of-Publication", "Publisher"], "outer"
)
df_final = (
    df_joined.withColumn(
        "Rating-Books-scores",
        (
            f.col("Number-of-ratings") * f.col("Rating-Average")
            + (f.col("Avg-note") * f.col("min-votes-required"))
        )
        / (f.col("min-votes-required") + f.col("Number-of-ratings")),
    )
    .sort(f.desc("Rating-Books-scores"))
    .select("Year-Of-Publication", "Publisher", "Book-Author", "Rating-Books-scores")
)

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("authors_pub_years_statistics")
