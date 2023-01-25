# Databricks notebook source
# MAGIC %run ../initial_notebook

# COMMAND ----------

from pyspark.sql.functions import col, avg

df_books_rating_cleaned = spark.table("book_ratings_silver")
df_books_cleaned = spark.table("books_silver")

df_author_number_of_readers = df_books_rating_cleaned.join(df_books_cleaned, "ISBN").groupBy('Book-Author').agg(count('Book-Author').alias("Nr-Of-Ratings"))
df_author_number_of_readers = df_author_number_of_readers.sort('Nr-Of-Ratings', ascending=[False])
df_author_number_of_readers = df_author_number_of_readers.where(col('Nr-Of-Ratings') > 50)

# COMMAND ----------

df_author_rating = (df_books_rating_cleaned.join(df_books_cleaned, "ISBN")
                    .groupBy('Book-Author').agg(avg('Book-Rating').alias('Average-Rating')))
display(df_author_rating)

# COMMAND ----------

df_answer_ex_1 = (df_author_number_of_readers.join(df_author_rating, 'Book-Author')
                  .sort(['Average-Rating', 'Nr-Of-Ratings'], ascending=[False, False]))
display(df_answer_ex_1)

# COMMAND ----------

(df_answer_ex_1.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("path", f'{answer_question}/best_author')
 .saveAsTable("best_author_answer"))
