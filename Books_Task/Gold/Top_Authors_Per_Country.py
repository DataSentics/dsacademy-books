# Databricks notebook source
# MAGIC %run ../Initializing_Notebook

# COMMAND ----------

book_user_ratings = spark.read.format('delta').load(f'{silver_files}/Books_User_Ratings')
best_authors_bayesian = spark.read.format('delta').load(f'{gold_path}/best_authors_bayesian')
best_books_bayesian = spark.read.format('delta').load(f'{gold_path}/best_books_bayesian')

# COMMAND ----------

author_per_country = (book_user_ratings
                      .groupBy('country', 'Book_Author').count())

display(author_per_country)

# COMMAND ----------

top_authors_per_country = (author_per_country
                           .groupBy('country', 'Book_Author').agg({'count': 'sum'})
                           .sort(f.col('sum(count)').desc())
                           .groupBy('country').agg(f.collect_list('Book_Author'))
                           .withColumnRenamed('collect_list(Book_Author)', 'Top_Authors')
                           .withColumn('Top_Authors', f.slice('Top_Authors', 1, 10))
                           .withColumn('Top_Author', f.col('Top_Authors')[0])
                           .withColumn('country', f.initcap(f.col('country')))
                           .withColumn('country', f.when(f.col('country') == 'Usa',
                                                         'United States').otherwise(f.col('country'))))

display(top_authors_per_country)
