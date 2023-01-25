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

df_gold_avg_book_rating_per_gender_age = (spark
                                          .table('silver_book_ratings')
                                          .join(spark.table('silver_books'), 'ISBN', 'inner')
                                          .join(spark.table('silver_users'), 'User-ID', 'inner')
                                          .withColumn('Age-Group',
                                                      f.when(f.col('Age') < 100,
                                                             f.concat(
                                                                 ((f.col('Age') / 10).cast('integer') * 10 + 1)
                                                                 .cast('string'),
                                                                 f.lit('-'),
                                                                 (((f.col('Age') / 10).cast('integer') + 1) * 10)
                                                                 .cast('string')))
                                          .when(f.col('Age') >= 100, 'Over100').otherwise(None))
                                          .groupBy('Gender', 'Age-Group')
                                          .agg(f.round(f.avg('Book-Rating'), 2).alias('Average-Rating'))
                                          .sort(f.col('Gender'), f.col('Age-Group')))

# COMMAND ----------

# MAGIC %md
# MAGIC # Write table

# COMMAND ----------

(df_gold_avg_book_rating_per_gender_age
 .write
 .format('delta')
 .mode('overwrite')
 .option('path', m.gold_avg_book_rating_GA_path)
 .saveAsTable('gold_avg_book_rating_per_gender_age'))
