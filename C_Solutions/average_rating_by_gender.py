# Databricks notebook source
# MAGIC %run ../initial_notebook

# COMMAND ----------

# from pyspark.sql.functions import avg, col, lit, when

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


df = (spark.read.format('delta').load(
    f'{path_to_cleansed_storage}/users_pii_silver').join(spark.read.format('delta').load(
        f'{path_to_cleansed_storage}/book_ratings_silver'), "User-ID"))

df = (df
      .filter(col('age').isNotNull())
      .withColumn('Age_Group', age_groups(col("age")))
      .groupBy('gender', 'Age_Group').agg(avg('Book-Rating').cast('decimal(9, 2)')).alias("Average-Book-Rating")
      .sort('gender', 'Age_Group'))

display(df)

# COMMAND ----------

# df_users = spark.read.format('delta').load(
#     f'{path_to_cleansed_storage}/users_pii_silver')
# df_ratings = spark.read.format('delta').load(
#     f'{path_to_cleansed_storage}/book_ratings_silver')
# df_merged = df_users.join(df_ratings, "User-ID")

# df_merged = (df_merged.filter(col('age').isNotNull())
#                       .withColumn("Age_Group", when(col("age") < 11, lit("1-10"))
#                                   .otherwise(when(col("age") < 21, lit("11-20"))
#                                   .otherwise(when(col("age") < 31, lit("21-30"))
#                                   .otherwise(when(col("age") < 41, lit("31-40"))
#                                   .otherwise(when(col("age") < 51, lit("41-50"))
#                                   .otherwise(when(col("age") < 61, lit("51-60"))
#                                   .otherwise(when(col("age") < 71, lit("61-70"))
#                                   .otherwise(when(col("age") < 81, lit("71-80"))
#                                   .otherwise(when(col("age") < 91, lit("81-90"))
#                                   .otherwise(when(col("age") < 101, lit("91-100"))
#                                   .otherwise(when(col("age") > 100, lit("_101+")))))))))))))
#                       .groupBy('gender', 'Age_Group').agg(avg('Book-Rating').alias("Average-Book-Rating"))
#                       .withColumn('Average-Book-Rating', col('Average-Book-Rating').cast('decimal(9, 2)'))
#                       .sort('gender', 'Age_Group'))

# display(df_merged)


# COMMAND ----------

(df_merged.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("path", f'{answer_question}/ratings_by_gender')
 .saveAsTable("ratings_by_gender_answer"))
