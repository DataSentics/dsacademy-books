# Databricks notebook source
# MAGIC %md 
# MAGIC ##Owner : Anistoroaei Nicole-Laurian
# MAGIC ##Goal : show the top 10 best books by country
# MAGIC

# COMMAND ----------

# MAGIC %run /Repos/Book_Task/dsacademy-books/Notebooks/BronzeLayer/Utilities/db_notebook

# COMMAND ----------


import GoldUtilities.utilities as u
from pyspark.sql import functions as f
from pyspark.sql.window import Window

# COMMAND ----------

df_silver_joins = spark.read.format("delta").load(u.joins_path)

df_gold_most_rated_books_country = (df_silver_joins
                                    .filter(f.col('BookRating') > 0)
                                    .filter("Location is not NULL")
                                    .select(f.col('Location'), 
                                                           f.col('ISBN'), 
                                                           f.col('Location'),
                                                           f.col('BookTitle'), 
                                                           f.col('BookAuthor'), 
                                                           f.col('BookRating'), 
                                                           f.col('YearOfPublication'))
                                    .groupBy(['ISBN', 'Location', 'BookTitle', 'BookAuthor', 'YearOfPublication'])
                                    .agg(f.mean(f.col('BookRating')).alias('AvgRating'), f.count('BookRating').alias('RatingNo'))
                                    .orderBy(f.col('RatingNo').desc(),f.col('AvgRating').desc())                                                               
)
df_gold_most_rated_books_country.write.format('delta').mode('overwrite').save(u.top_most_rated_per_country)



# COMMAND ----------


