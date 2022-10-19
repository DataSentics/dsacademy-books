# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

year_publisher_ratings = spark.table("authors_pub_years")

intermediar_df = year_publisher_ratings.groupBy("Year-Of-Publication", "Publisher").agg(
    f.avg("Number-of-ratings").cast("int").alias("min-votes-required"),
    f.avg("Rating-Average").alias("Avg-note"),
)

# COMMAND ----------

df_joined = year_publisher_ratings.join(
    intermediar_df, ["Year-Of-Publication", "Publisher"], "outer"
)
df_final = (
    df_joined.withColumn(
        "Rating-for-statistics",
        (
            f.col("Number-of-ratings") * f.col("Rating-Average")
            + (f.col("Avg-note") * f.col("min-votes-required"))
        )
        / (f.col("min-votes-required") + f.col("Number-of-ratings")),
    )
    .sort(f.desc("Rating-for-statistics"))
    .drop(f.col("Number-of-ratings"))
    .drop(f.col("Rating-Average"))
    .drop(f.col("min-votes-required"))
    .drop(f.col("Avg-note"))
)

# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("authors_pub_years_statistics")
