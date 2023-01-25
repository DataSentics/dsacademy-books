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

def top10_authors_custom(dfs, age_category, gender, country):

    df_joined = (dfs[0]
                 .join(dfs[1], 'ISBN', 'inner')
                 .join(dfs[2], 'User-ID', 'inner')
                 .filter((f.col('Age') >= age_category[0])
                         & (f.col('Age') <= age_category[1])
                         & (f.col('Gender') < gender)
                         & (f.col('Country') < country))
                 .groupBy('Book-Author')
                 .agg(f.count('Book-Rating').cast('integer').alias('Count-Ratings'),
                      f.round(f.avg('Book-Rating'), 2).alias('Average-Rating'))
                 .sort(f.col('Count-Ratings').desc())
                 .filter(f.col('Average-Rating') >= 7)
                 .limit(10)
                 .sort(f.col('Average-Rating').desc()))

    return df_joined

# COMMAND ----------

# MAGIC %md
# MAGIC # Write table

# COMMAND ----------

(top10_authors_custom([spark.table('silver_book_ratings'),
                       spark.table('silver_books'),
                       spark.table('silver_users')],
                      [20, 30],
                      'M',
                      'Usa')
 .write
 .format('delta')
 .mode('overwrite')
 .option('path', m.gold_top10_authors_AGC_path)
 .saveAsTable('gold_top10_authors_for_age_gender_country'))
