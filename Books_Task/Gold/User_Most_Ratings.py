# Databricks notebook source
# MAGIC %run ../Initializing_Notebook

# COMMAND ----------

# Creating a dataframe containing the user_ratings table and books_silver table

user_ratings = spark.read.format('delta').load(f'{silver_files}/User_Ratings')
books = spark.read.format('delta').load(books_silver_path)

# COMMAND ----------

display(user_ratings)

# COMMAND ----------

# Finding the highest rater

highest_rater = (user_ratings
                 .join(books, 'ISBN', 'inner')
                 .sort(f.col('Book_Title'))
                 .drop('Age', 'City', 'Region', 'Publisher')
                 .withColumnRenamed('avg(Book_Rating)', 'Average_rating')
                 .filter(f.col('Year_of_publication') >= 2000)
                 .groupBy('User_ID', 'Country').count()
                 .withColumnRenamed('count', 'Number_of_reviews')
                 .sort(f.col('count').desc())
                 .limit(1))

# COMMAND ----------

# Displaying the highest rater

print('The user with most reviews given is from', f'{highest_rater.collect()[0][1]},',
      'with', highest_rater.collect()[0][2], 'total book reviews.')

# COMMAND ----------

# Saving highest_rater to path

highest_rater.write.format('delta').mode('overwrite').save(f'{gold_path}/highest_rater')
