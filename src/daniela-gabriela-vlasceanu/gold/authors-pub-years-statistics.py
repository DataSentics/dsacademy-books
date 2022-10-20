# Databricks notebook source
# MAGIC %run ../variables

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

# CREATING THE COLUMNS FOR THE NEXT DF, WITH NUM_OF_MIN_RATINGS AND AVG_RATING_IN_TOTAL
year_publisher_ratings = spark.table("authors_pub_years")
intermediar_df = (
    year_publisher_ratings
    .groupBy("Year_Of_Publication", "Publisher")
    .agg(
        f.avg("Number_of_ratings").cast("int").alias("min_votes_required"),
        f.avg("Rating_Average").alias("Avg_note")
    )
)

# COMMAND ----------

# Using the IMDB rating formula to calculate:
# (num_ratings * avg_rating_by_year_publisher + min_num_ratings * avg_ratings_total)/(num_ratings + min_num_ratings)
df_joined = year_publisher_ratings.join(
    intermediar_df, ["Year_Of_Publication", "Publisher"], "outer"
)
df_final = (
    df_joined.withColumn(
        "Rating_Books_scores",
        (
            f.col("Number_of_ratings") * f.col("Rating_Average")
            + (f.col("Avg_note") * f.col("min_votes_required"))
        )
        / (f.col("min_votes_required") + f.col("Number_of_ratings")),
    )
    .sort(f.desc("Rating_Books_scores"))
    .select(
        "Year_Of_Publication",
        "Publisher",
        "Book_Author",
        "Rating_Books_scores"
    )
)

# COMMAND ----------

upload_path = (
    f"{azure_storage}".format("04golddata")
    + "daniela-vlasceanu-books/gold/authors_pub_years_statistics"
)

# COMMAND ----------

(
    df_final
    .write
    .format("delta")
    .mode("overwrite")
    .option("path", upload_path)
    .saveAsTable("authors_pub_years_statistics")
)
