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
m = df.select("min-votes-required").first()[0]
C = df.select("Avg-note").first()[0]

# COMMAND ----------

df_final = (
    authors_ratings_df.withColumn(
        "Rating-Books-scores",
        (f.col("Number-of-ratings") * f.col("Rating-Average") + C * m)
        / (m + f.col("Number-of-ratings")),
    )
    .sort(f.desc("Rating-Books-scores"))
    .select("Book-Author","Rating-Books-scores")
)

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("authors_ratings_for_statistics")
