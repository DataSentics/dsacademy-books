# Databricks notebook source
# MAGIC %run ../Set_paths/silver_paths

# COMMAND ----------

df_book_ratings = (
    spark.readStream.table("bronze_ratings")
    .withColumnRenamed("User-ID", "User_ID")
    .withColumnRenamed("Book-Rating", "Book_Rating")
    .withColumn("Book_Rating", col("Book_Rating").cast("Integer"))
)

# COMMAND ----------

(
    df_book_ratings.writeStream.format("delta")
    .option("checkpointLocation", ratings_checkpoint)
    .option("path", ratings_output_path)
    .trigger(availableNow=True)
    .outputMode("append")
    .table("silver_ratings")
)
