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
# MAGIC # Answer question

# COMMAND ----------

def top10_books_in_period(dfs, period):

    df_joined =(dfs[0]
                .join(dfs[1], 'ISBN', 'inner')
                .filter((f.col('Year-Of-Publication') > period[0])
                        & (f.col('Year-Of-Publication') < period[1]))
                .groupBy('ISBN', 'Book-Title', 'Year-Of-Publication')
                .agg(f.count('Book-Rating').cast('integer').alias('Count-Ratings'),
                     f.round(f.avg('Book-Rating'), 2).alias('Avg-Ratings'))
                .sort(f.col('Count-Ratings').desc())
                .filter(f.col('Avg-Ratings') >= 7)
                .limit(10)
                .sort(f.col('Avg-Ratings').desc()))

    return df_joined

# COMMAND ----------

# MAGIC %md
# MAGIC # Write table

# COMMAND ----------

m.write_table(top10_books_in_period([spark.table('silver_book_ratings'),
                                     spark.table('silver_books')],
                                    [2000, 2023]),
              m.gold_top10_books_period_path,
              'gold_top10_books_period')
