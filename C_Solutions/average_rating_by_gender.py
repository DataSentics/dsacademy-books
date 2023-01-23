# Databricks notebook source
# MAGIC %run ../initial_notebook

# COMMAND ----------

from pyspark.sql.functions import avg, col

# COMMAND ----------

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

df_users = spark.read.format('delta').load(
    f'{path_to_cleansed_storage}/users_pii_silver')
df_ratings = spark.read.format('delta').load(
    f'{path_to_cleansed_storage}/book_ratings_silver')
df_merged = df_users.join(df_ratings, "User-ID")

df_merged = (df_merged
             .filter(col('age').isNotNull())
             .withColumn('Age_Group', age_groups(col("age")))
             .groupBy('gender', 'Age_Group').agg(avg('Book-Rating').alias("Average-Book-Rating"))
             .withColumn('Average-Book-Rating', col('Average-Book-Rating').cast('decimal(9, 2)'))
             .sort('gender', 'Age_Group'))

display(df_merged)

# COMMAND ----------

(df_merged.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("path", f'{answer_question}/ratings_by_gender')
 .saveAsTable("ratings_by_gender_answer"))
