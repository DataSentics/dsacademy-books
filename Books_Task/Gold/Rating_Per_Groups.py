# Databricks notebook source
# MAGIC %run ../Initializing_Notebook

# COMMAND ----------

# Creating dataframe containing the User_Ratings table

user_ratings = spark.read.format('delta').load(f'{silver_files}/User_Ratings')

# COMMAND ----------

# Defining age groups

age_groups = udf(lambda age: '01_10' if age <= 10 else
                 '11_20' if (age > 10 and age <= 20) else
                 '21-30' if (age > 20 and age <= 30) else
                 '31-40' if (age > 30 and age <= 40) else
                 '41-50' if (age > 40 and age <= 50) else
                 '51-60' if (age > 50 and age <= 60) else
                 '61-70' if (age > 60 and age <= 70) else
                 '71-80' if (age > 70 and age <= 80) else
                 '81_90' if (age > 80 and age <= 90) else
                 '91_100' if (age > 90 and age <= 100) else
                 '_100 +' if (age > 100) else '')

# COMMAND ----------

# Getting the average ratings per gender and age groups

average_per_group = (user_ratings
                     .filter(f.col('Age').isNotNull())
                     .withColumn('Age_Group', age_groups(user_ratings.Age))
                     .groupBy('gender', 'Age_Group').avg('Book_Rating')
                     .withColumnRenamed('avg(Book_Rating)', 'Average_Rating')
                     .withColumnRenamed('gender', 'Gender')
                     .withColumn('Average_Rating', f.col('Average_Rating').cast('decimal(9, 2)'))
                     .sort('gender', 'Age_Group'))

display(average_per_group)

# COMMAND ----------

# Saving average_per_group to path

average_per_group.write.format('delta').mode('overwrite').save(f'{gold_path}/average_per_group')
