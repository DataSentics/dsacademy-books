# Databricks notebook source
# MAGIC %run ../Initializing_Notebook

# COMMAND ----------

spark.conf.set("fs.azure.account.key.adapeuacadlakeg2dev.dfs.core.windows.net",
               "wA432KewaHRxET7kpSgyAAL6/6u031XV+wA0x/3P3UGbJLxNPxA30VBHO8euadaQ/Idcl+vGujvd+AStK8VTHg==")

# COMMAND ----------

book_user_ratings = spark.read.format('delta').load(f'{silver_files}/Books_User_Ratings')

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
