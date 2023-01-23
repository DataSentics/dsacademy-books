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

df_top10_authors = (spark.table('silver_books')
                    .join(spark.table('silver_book_ratings'), 'ISBN', 'inner')
                    .groupBy('Book-Author')
                    .agg(f.count('Book-Rating').cast('integer').alias('Count-Ratings'),
                         f.round(f.avg('Book-Rating'), 2).alias('Avg-Ratings'))
                    .sort(f.col('Count-Ratings').desc())
                    .filter(f.col('Avg-Ratings') >= 7)
                    .limit(10)
                    .sort(f.col('Avg-Ratings').desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC # Write table

# COMMAND ----------

m.write_table(df_top10_authors, m.gold_top10_authors_path, 'gold_top10_authors')
