# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

authors_ratings_df = spark.table("ratings_authors")

df = authors_ratings_df.agg(
    f.avg("Number_of_ratings").cast("int").alias("min_votes_required"),
    f.avg("Rating_Average").alias("Avg_note"),
)
m = df.select("min_votes_required").first()[0]
C = df.select("Avg_note").first()[0]

# COMMAND ----------

df_final = (
    authors_ratings_df.withColumn(
        "Rating_Books_scores",
        (f.col("Number_of_ratings") * f.col("Rating_Average") + C * m)
        / (m + f.col("Number_of_ratings")),
    )
    .sort(f.desc("Rating_Books_scores"))
    .select("Book_Author", "Rating_Books_scores")
)

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("authors_ratings_for_statistics")
