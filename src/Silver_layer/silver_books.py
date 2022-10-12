# Databricks notebook source
# MAGIC %run ../Set_paths/silver_paths

# COMMAND ----------

df_books = (
    spark.readStream.table("bronze_books")
    .withColumnRenamed("Book-Title", "Book_Title")
    .withColumnRenamed("Book-Author", "Book_Author")
    .withColumnRenamed("Year-Of-Publication", "Year_Of_Publication")
    .withColumnRenamed("Image-URL-S", "Image_RL_S")
    .withColumnRenamed("Image-URL-M", "Image_URL_M")
    .withColumnRenamed("Image-URL-L", "Image_URL_L")
    .withColumn(
        "Year_Of_Publication",
        when(col("Year_Of_Publication") == "0", "Unknown").otherwise(
            col("Year_Of_Publication")
        ),
    )
    .fillna("unknown")
)

# COMMAND ----------

(
    df_books.writeStream.format("delta")
    .option("checkpointLocation", books_checkpoint)
    .option("path", books_output_path)
    .trigger(availableNow=True)
    .outputMode("append")
    .table("silver_books")
)
