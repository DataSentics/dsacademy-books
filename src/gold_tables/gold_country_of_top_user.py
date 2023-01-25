# Databricks notebook source
# MAGIC %md
# MAGIC # Import necessary modules

# COMMAND ----------

import pyspark.sql.functions as f
import mypackage.mymodule as m

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
                          .agg(f.count("Book-Rating").cast('integer').alias("Number-Of-Ratings"))
                          .sort(f.col("Number-Of-Ratings").desc())
                          .limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC # Write table

# COMMAND ----------

(df_country_of_top_user
 .write
 .format('delta')
 .mode('overwrite')
 .option('path', m.gold_country_of_top_user_path)
 .saveAsTable('gold_country_of_top_user'))
