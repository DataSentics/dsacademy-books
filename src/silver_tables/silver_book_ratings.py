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
# MAGIC # Read and clean table

# COMMAND ----------

silver_df_book_ratings = (spark
                          .table('bronze_book_ratings')
                          .withColumn('User-ID', f.col('User-ID').cast('integer'))
                          .withColumn('Book-Rating', f.col('Book-Rating').cast('integer'))
                          .filter(f.col("Book-Rating") > 0)
                          .withColumn('ISBN', f.trim(f.col("ISBN"))))

# COMMAND ----------

# MAGIC %md
# MAGIC # Write table

# COMMAND ----------

(silver_df_book_ratings
 .write
 .format('delta')
 .mode('overwrite')
 .option('path', m.silver_book_ratings_path)
 .saveAsTable('silver_book_ratings'))
