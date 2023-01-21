# Databricks notebook source
# MAGIC %run ../Initializing_Notebook

# COMMAND ----------

spark.conf.set("fs.azure.account.key.adapeuacadlakeg2dev.dfs.core.windows.net",
               "wA432KewaHRxET7kpSgyAAL6/6u031XV+wA0x/3P3UGbJLxNPxA30VBHO8euadaQ/Idcl+vGujvd+AStK8VTHg==")

# COMMAND ----------

book_user_ratings = spark.read.format('delta').load(f'{silver_files}/Books_User_Ratings')

# COMMAND ----------

books_per_country = (book_user_ratings
                     .groupBy('country', 'ISBN').count())

display(books_per_country)

# COMMAND ----------

top_books_per_country = (books_per_country
                         .groupBy('country', 'ISBN').agg({'count': 'sum'})
                         .sort(f.col('sum(count)').desc())
                         .groupBy('country').agg(f.collect_list('ISBN'))
                         .withColumnRenamed('country', 'country_temp')
                         .withColumnRenamed('collect_list(ISBN)', 'Top_Books')
                         .withColumn('Top_Books', f.slice('Top_Books', 1, 10))
                         .withColumn('ISBN', f.col('Top_Books')[0])
                         .withColumn('country_temp', f.initcap(f.col('country_temp')))
                         .withColumn('country_temp', f.when(f.col('country_temp') == 'Usa',
                                                       'United States').otherwise(f.col('country_temp')))
                         .join(book_user_ratings, 'ISBN', 'inner')
                         .select('country_temp', 'Top_Books', 'ISBN', 'Book_Title', 'Book_Author')
                         .withColumnRenamed('country_temp', 'country')
                         .withColumnRenamed('ISBN', 'Top_Book')
                         .groupBy('country', 'Top_Books', 'Top_Book', 'Book_Title', 'Book_Author').count()
                         .drop('count')
                         .sort('country'))

display(top_books_per_country)
