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

df_top10_authors = (spark.table('silver_books')
                    .join(spark.table('silver_book_ratings'), 'ISBN', 'inner')
                    .groupBy('Book-Author')
                    .agg(f.count('Book-Rating').cast('integer').alias('Count-Ratings'),
                         f.round(f.avg('Book-Rating'), 2).alias('Average-Rating'))
                    .sort(f.col('Count-Ratings').desc())
                    .filter(f.col('Average-Rating') >= 7)
                    .limit(10)
                    .sort(f.col('Average-Rating').desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC # Write table

# COMMAND ----------

(df_top10_authors
 .write
 .format('delta')
 .mode('overwrite')
 .option('path', m.gold_top10_authors_path)
 .saveAsTable('gold_top10_authors'))
