# Databricks notebook source
# MAGIC %md
# MAGIC # Import necessary modules

# COMMAND ----------

import pyspark.sql.functions as f
import mypackage.mymodule as m

# COMMAND ----------

# MAGIC %md
# MAGIC # Run initial setup

# COMMAND ----------

# MAGIC %run ../use_database

# COMMAND ----------

# MAGIC %md
# MAGIC # Answer question

# COMMAND ----------

df_country_of_top_user = (spark
                          .table("silver_books")
                          .join(spark.table("silver_book_ratings"), "ISBN", "inner")
                          .join(spark.table("silver_users"), "User-ID", "inner")
                          .filter(f.col("Year-Of-Publication") > 2000)
                          .groupBy("User-ID", "Country")
                          .agg(f.count("Book-Rating").cast('integer').alias("Nr-Of-Ratings"))
                          .sort(f.col("Nr-Of-ratings").desc())
                          .limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC # Write table

# COMMAND ----------

m.write_table(df_country_of_top_user, m.gold_country_of_top_user_path, 'gold_country_of_top_user')
