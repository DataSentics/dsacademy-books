# Databricks notebook source
# A new analytical query: What is the average book rating per gender 
# (men vs. women) and per age group (age groups are: 0-10, 11-20, 21-30...)

# COMMAND ----------

# MAGIC %sql
# MAGIC --the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

# getting the tables from the metastore and saving them into 3 dataframes
df_users = spark.sql("select * from ANiteanuBooks.users_3nf")
df_rating = spark.sql("select * from ANiteanuBooks.books_silver")
df_books = spark.sql("select * from ANiteanuBooks.books_rating_silver")

# COMMAND ----------

df = df_rating.join(df_books, on="ISBN")
df = df.join(df_users, on="User-ID")

# COMMAND ----------

# registering the joined table
df.write.mode("overwrite").saveAsTable("users_rating_books")
