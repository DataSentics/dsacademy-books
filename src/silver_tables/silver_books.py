# Databricks notebook source
# MAGIC %run ../init_setup

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

silver_df_books.write.format('delta').mode('overwrite').option('path', m.silver_books_path).saveAsTable('silver_books')
