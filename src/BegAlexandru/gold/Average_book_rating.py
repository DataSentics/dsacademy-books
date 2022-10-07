# Databricks notebook source
from pyspark.sql.functions import avg

# COMMAND ----------

# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

silver_books_df = spark.readStream.table("silver_books")
silver_ratings_df = spark.readStream.table("silver_ratings")

# COMMAND ----------

joined_df = silver_books_df.join(silver_ratings_df, on="ISBN")

# COMMAND ----------

# best-rated authors by year of publication and publishers
joined_df = (joined_df
             .groupBy("Year-Of-Publication", "Publisher", "Book-Author")
             .agg(avg("Book-Rating").alias("Book-Rating"))
            )
