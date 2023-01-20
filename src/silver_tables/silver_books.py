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

# MAGIC %run ../init_setup

# COMMAND ----------

# MAGIC %md
# MAGIC # Read and clean table

# COMMAND ----------

silver_df_books = (spark
                   .table('bronze_books')
                   .withColumn('Book-Title', f.trim(f.initcap(f.lower(f.col('Book-Title')))))
                   .withColumn('Book-Author', f.trim(f.initcap(f.lower(f.col('Book-Author')))))
                   .withColumn('Publisher', f.trim(f.initcap(f.lower(f.col('Publisher')))))
                   .withColumn('Year-Of-Publication', f.col('Year-Of-Publication').cast('integer'))
                   .withColumn('Year-Of-Publication', f.when((f.col('Year-Of-Publication') < 1376) |
                               (f.col('Year-Of-Publication') > 2021), None)
                               .otherwise(f.col('Year-Of-Publication'))))

# COMMAND ----------

# MAGIC %md
# MAGIC # Write table

# COMMAND ----------

m.write_table(silver_df_books, m.silver_books_path, 'silver_books')
