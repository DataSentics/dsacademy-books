# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

authors_ratings_df = spark.table("ratings_authors")

df = authors_ratings_df.agg(
    f.avg("Number-of-ratings").cast("int").alias("min-votes-required"),
    f.avg("Rating-Average").alias("Avg-note"),
)

# COMMAND ----------

min_votes_required = df.select("min-votes-required").collect()
m = min_votes_required[0].__getitem__("min-votes-required")

Avg_note = df.select("Avg-note").collect()
C = Avg_note[0].__getitem__("Avg-note")

# COMMAND ----------

df_final = (
    authors_ratings_df.withColumn(
        "Rating-for-statistics",
        (f.col("Number-of-ratings") * f.col("Rating-Average") + C * m)
        / (m + f.col("Number-of-ratings")),
    )
    .sort(f.desc("Rating-for-statistics"))
    .drop(f.col("Number-of-ratings"))
    .drop(f.col("Rating-Average"))
)

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("authors_ratings_for_statistics")
