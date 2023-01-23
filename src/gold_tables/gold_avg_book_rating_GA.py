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

df_avg_book_rating_GA = (spark
                         .table('silver_book_ratings')
                         .join(spark.table('silver_books'), 'ISBN', 'inner')
                         .join(spark.table('silver_users'), 'User-ID', 'inner')
                         .withColumn('Age-Group', f.when((f.col('Age') >= 0) & (f.col('Age') <= 10), '0-10')
                                     .when((f.col('Age') >= 0) & (f.col('Age') <= 10), '0-10')
                                     .when((f.col('Age') >= 11) & (f.col('Age') <= 20), '11-20')
                                     .when((f.col('Age') >= 21) & (f.col('Age') <= 30), '21-30')
                                     .when((f.col('Age') >= 31) & (f.col('Age') <= 40), '31-40')
                                     .when((f.col('Age') >= 41) & (f.col('Age') <= 50), '41-50')
                                     .when((f.col('Age') >= 51) & (f.col('Age') <= 60), '51-60')
                                     .when((f.col('Age') >= 61) & (f.col('Age') <= 70), '61-70')
                                     .when((f.col('Age') >= 71) & (f.col('Age') <= 80), '71-80')
                                     .when((f.col('Age') >= 81) & (f.col('Age') <= 90), '81-90')
                                     .when((f.col('Age') >= 91) & (f.col('Age') < 100), '91-99')
                                     .when(f.col('Age') >= 101, 'Centenarians').otherwise(None))
                         .groupBy('Gender', 'Age-Group')
                         .agg(f.round(f.avg('Book-Rating'), 2).alias('Avg-Rating-GA'))
                         .sort(f.col('Gender'), f.col('Age-Group')))

# COMMAND ----------

# MAGIC %md
# MAGIC # Write table

# COMMAND ----------

m.write_table(df_avg_book_rating_GA, m.gold_avg_book_rating_GA_path, 'gold_avg_book_rating_GA')
