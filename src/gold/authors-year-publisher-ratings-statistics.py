# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

year_publisher_ratings = spark.table("authors_pub_years")

df = year_publisher_ratings.groupBy("Year-Of-Publication", "Publisher").agg(
    f.avg("How_many_ratings").cast("int").alias("min_votes_required"),
    f.avg("Rating-Average").alias("Avg_note"),
)
display(df)

# COMMAND ----------

df_joined = year_publisher_ratings.join(
    df, ["Year-Of-Publication", "Publisher"], "outer"
)
df_final = (
    df_joined.withColumn(
        "Rating_for_statistics",
        (
            f.col("How_many_ratings") * f.col("Rating-Average")
            + (f.col("Avg_note") * f.col("min_votes_required"))
        )
        / (f.col("min_votes_required") + f.col("How_many_ratings")),
    )
    .sort(f.desc("Rating_for_statistics"))
    .drop(f.col("How_many_ratings"))
    .drop(f.col("Rating-Average"))
    .drop(f.col("min_votes_required"))
    .drop(f.col("Avg_note"))
)
display(df_final)

# COMMAND ----------

df_final.createOrReplaceTempView("authors_pub_years_tempView")
spark.sql(
    "CREATE OR REPLACE TABLE authors_pub_years_statistics AS SELECT * FROM authors_pub_years_tempView"
)
