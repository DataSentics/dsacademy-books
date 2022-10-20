# Databricks notebook source
# MAGIC %run ../variables

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

authors_ratings_df = spark.table("ratings_authors")

df = authors_ratings_df.agg(
    f.avg("Number_of_ratings").cast("int").alias("min_votes_required"),
    f.avg("Rating_Average").alias("Avg_note"),
)
intermediate_df = authors_ratings_df.join(df)

# COMMAND ----------

df_final = (
    intermediate_df
    .coalesce(1)
    .withColumn(
        "Rating_Books_scores",
        (f.col("Number_of_ratings") * f.col("Rating_Average")
         + f.col("Avg_note") * f.col("min_votes_required"))
        / (f.col("min_votes_required") + f.col("Number_of_ratings"))
    )
    .sort(f.desc("Rating_Books_scores"))
    .select("Book_Author", "Rating_Books_scores")
)

# COMMAND ----------

upload_path = (
    f"{azure_storage}".format("04golddata")
    + "daniela-vlasceanu-books/gold/authors_ratings_for_statistics"
)

# COMMAND ----------

(
    df_final
    .write
    .format("delta")
    .mode("overwrite")
    .option("path", upload_path)
    .saveAsTable("authors_ratings_for_statistics")
)
