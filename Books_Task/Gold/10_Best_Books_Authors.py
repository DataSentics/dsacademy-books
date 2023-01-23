# Databricks notebook source
# MAGIC %run ../Initializing_Notebook

# COMMAND ----------

# Assigning the needed tables to dataframes

book_user_ratings = spark.read.format('delta').load(f'{silver_files}/Books_User_Ratings')
books = spark.read.format('delta').load(f'{silver_files}/books_silver')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting up 10 best authors with Wilson Confidence Interval method

# COMMAND ----------

# Getting the number of positive and negative reviews per book
# Applying the Wilson Confidence Interval
# Finding the best books through this method

# This method returns generally considered best books, being weighted
# on the number of positive reviews compared to the total number of reviews

best_books_wilson = (book_user_ratings
                     .withColumn('Positive_review', f.when(f.col('Book_Rating') >= 8, 1).otherwise(0))
                     .withColumn('Negative_review', f.when(f.col('Book_Rating') < 8, 1).otherwise(0))
                     .groupBy('ISBN').agg({'ISBN': 'count', 'Book_Rating': 'avg',
                                           'Positive_review': 'sum', 'Negative_review': 'sum'})
                     .withColumnRenamed('avg(Book_Rating)', 'Average_rating')
                     .withColumn('Average_rating', f.col('Average_rating').cast('decimal(9, 2)'))
                     .withColumnRenamed('count(ISBN)', 'Total_reviews')
                     .withColumnRenamed('sum(Negative_review)', 'Negative_reviews')
                     .withColumnRenamed('sum(Positive_review)', 'Positive_reviews')
                     .withColumn('Wilson_confidence', udf_wilson(f.col('Positive_reviews'), f.col('Total_reviews')))
                     .withColumn('Wilson_confidence', f.col('Wilson_confidence').cast('decimal(9, 4)'))
                     .join(books, 'ISBN', 'inner')
                     .select('ISBN', 'Wilson_confidence', 'Average_rating', 'Total_reviews',
                             'Book_Title', 'Book_Author', 'Year_of_publication', 'Publisher')
                     .sort(f.col('Wilson_confidence').desc()))

display(best_books_wilson)
best_books_wilson.printSchema()

# COMMAND ----------

# Getting the total book number to weight the average
# Wilson score on number of books written by each author

book_number = (best_books_wilson
               .select('Book_Author', 'ISBN')
               .groupBy('Book_Author').count())

# COMMAND ----------

# Obtaining the final score and best authors sorted

best_authors_wilson = (best_books_wilson
                       .groupBy('Book_Author').agg(f.avg('Wilson_confidence'))
                       .join(book_number, 'Book_Author', 'inner')
                       .withColumnRenamed('count', 'Books_written')
                       .withColumnRenamed('avg(Wilson_confidence)', 'Wilson_score')
                       .withColumn('Final_score', f.col('Wilson_score') * f.col('Books_written'))
                       .drop('Wilson_score')
                       .sort(f.col('Final_score').desc()))


display(best_authors_wilson)
best_authors_wilson.count()

# COMMAND ----------

# Saving best_books_wilson and best_authors_wilson to path

best_books_wilson.write.format('delta').mode('overwrite').save(f'{gold_path}/best_books_wilson')
best_authors_wilson.write.format('delta').mode('overwrite').save(f'{gold_path}/best_authors_wilson')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setting up 10 best authors with Bayesian Credible Interval method

# COMMAND ----------

# Applying the Bayesian Credible Interval
# Finding the best books through this method

# This method returns the 'best popular books', being heavily weighted
# on number of reviews

best_books_bayesian = (book_user_ratings
                       .groupBy('ISBN', 'Book_Title', 'Book_Author', 'Year_of_publication', 'Publisher')
                       .agg(f.collect_list('Book_Rating'), f.avg('Book_Rating'))
                       .withColumnRenamed('collect_list(Book_Rating)', 'Ratings')
                       .withColumnRenamed('avg(Book_Rating)', 'Average_rating')
                       .filter(f.col('Average_rating') >= 8)
                       .withColumn('Bayesian_score', udf_bayesian(f.col('Ratings')))
                       .withColumn('Bayesian_score', f.col('Bayesian_score').cast('decimal(9, 4)'))
                       .withColumn('Average_rating', f.col('Average_rating').cast('decimal(9, 2)'))
                       .sort(f.col('Bayesian_score').desc(), f.col('Average_rating').desc())
                       .select('ISBN', 'Bayesian_score', 'Average_rating', 'Book_Title',
                               'Book_Author', 'Year_of_publication', 'Publisher')
                       .drop('Ratings'))

display(best_books_bayesian)

# COMMAND ----------

# Obtaining the final score and best authors sorted

best_authors_bayesian = (best_books_bayesian
                         .groupBy('Book_Author').agg(f.avg('Bayesian_score'))
                         .join(book_number, 'Book_Author', 'inner')
                         .withColumnRenamed('count', 'Books_written')
                         .withColumnRenamed('avg(Bayesian_score)', 'Bayesian_score')
                         .withColumn('Final_score', f.col('Bayesian_score') * f.col('Books_written'))
                         .drop('Bayesian_score')
                         .withColumnRenamed('Final_score', 'Weighted_score')
                         .sort(f.col('Weighted_score').desc()))

display(best_authors_bayesian)

# COMMAND ----------

best_authors_bayesian_test = (best_books_bayesian
                              .groupBy('Book_Author').agg(f.sum('Bayesian_score'))
                              .join(book_number, 'Book_Author', 'inner')
                              .withColumnRenamed('count', 'Books_written')
                              .withColumnRenamed('sum(Bayesian_score)', 'Bayesian_score')
                              .sort(f.col('Bayesian_score').desc()))

display(best_authors_bayesian_test)

# COMMAND ----------

# Saving best_books_bayesian and best_authos_wilson to path

best_books_bayesian.write.format('delta').mode('overwrite').save(f'{gold_path}/best_books_bayesian')
best_authors_bayesian.write.format('delta').mode('overwrite').save(f'{gold_path}/best_authors_bayesian')
