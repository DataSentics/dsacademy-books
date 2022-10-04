# Databricks notebook source
# joining all the tables

# COMMAND ----------

# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

nf3_df = spark.sql("SELECT * FROM 3nf_users")
ratings_df = spark.sql("SELECT * FROM silver_rating")
books_df = spark.sql("SELECT * FROM silver_books")

# COMMAND ----------

joined_df = ratings_df.join(books_df, on = 'ISBN').join(nf3_df, on = 'User-ID')

# COMMAND ----------

joined_df.write.mode("overwrite").saveAsTable("joined_books")

# COMMAND ----------

NF3_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "BegAlex_Books/3NF/joined_books"
)

# COMMAND ----------

joined_df.write.parquet(NF3_path, mode='overwrite')
