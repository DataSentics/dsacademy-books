# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

df_books = spark.table("books_silver")
df_books_ratings = spark.table("books_ratings_silver")
# display(df_books)
# display(df_books_ratings)

books_joined = df_books.join(df_books_ratings, "ISBN")

# COMMAND ----------

books_joined_df = books_joined.groupBy("Book-Author").agg(
    f.count("User-ID").alias("How_many_ratings"),
    f.avg("Book-Rating").alias("Rating-Average"),
)
# display(books_joined_df)

# COMMAND ----------

df = books_joined_df.agg(
    f.avg("How_many_ratings").cast("int").alias("min_votes_required"),
    f.avg("Rating-Average").alias("Avg_note"),
)
# display(df)

# COMMAND ----------

min_votes_required = df.select("min_votes_required").collect()
m = min_votes_required[0].__getitem__("min_votes_required")

Avg_note = df.select("Avg_note").collect()
C = Avg_note[0].__getitem__("Avg_note")

# COMMAND ----------

df_final = (
    books_joined_df.withColumn(
        "Raing_for_statistics",
        (f.col("How_many_ratings") * f.col("Rating-Average") + C * m)
        / (m + f.col("How_many_ratings")),
    )
    .sort(f.desc("Raing_for_statistics"))
    .drop(f.col("How_many_ratings"))
    .drop(f.col("Rating-Average"))
    .limit(10)
)
display(df_final)
