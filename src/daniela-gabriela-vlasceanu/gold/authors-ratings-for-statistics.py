# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

authors_ratings_df = spark.table("ratings_authors")

df = authors_ratings_df.agg(
    f.avg("Number_of_ratings").cast("int").alias("min_votes_required"),
    f.avg("Rating-Average").alias("Avg_note"),
)

# COMMAND ----------

min_votes_required = df.select("min_votes_required").collect()
m = min_votes_required[0].__getitem__("min_votes_required")

Avg_note = df.select("Avg_note").collect()
C = Avg_note[0].__getitem__("Avg_note")

# COMMAND ----------

df_final = (
    authors_ratings_df.withColumn(
        "Rating_for_statistics",
        (f.col("Number_of_ratings") * f.col("Rating-Average") + C * m)
        / (m + f.col("Number_of_ratings")),
    )
    .sort(f.desc("Rating_for_statistics"))
    .drop(f.col("Number_of_ratings"))
    .drop(f.col("Rating-Average"))
)

# COMMAND ----------

df_final.createOrReplaceTempView("ratings_authors_TempView")
spark.sql(
    "CREATE OR REPLACE TABLE authors_ratings_for_statistics AS SELECT * FROM ratings_authors_TempView"
)
