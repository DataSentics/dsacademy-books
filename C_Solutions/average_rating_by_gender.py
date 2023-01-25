# Databricks notebook source
# MAGIC %run ../initial_notebook

# COMMAND ----------

from pyspark.sql.functions import avg, col, lit, when

# COMMAND ----------

df_users = spark.read.format('delta').load(
    f'{path_to_cleansed_storage}/users_pii_silver')
df_ratings = spark.read.format('delta').load(
    f'{path_to_cleansed_storage}/book_ratings_silver')
df_merged = df_users.join(df_ratings, "User-ID")

df_merged = (df_merged
             .filter(col('age').isNotNull())
             .withColumn("Age_Group", when(col("age") < 11, lit("1-10"))
             .otherwise(when(col("age") < 21, lit("11-20"))
             .otherwise(when(col("age") < 31, lit("21-30"))
             .otherwise(when(col("age") < 41, lit("31-40"))
             .otherwise(when(col("age") < 51, lit("41-50"))
             .otherwise(when(col("age") < 61, lit("51-60"))
             .otherwise(when(col("age") < 71, lit("61-70"))
             .otherwise(when(col("age") < 81, lit("71-80"))
             .otherwise(when(col("age") < 91, lit("81-90"))
             .otherwise(when(col("age") < 101, lit("91-100"))
             .otherwise(when(col("age") > 100, lit("_101+")))))))))))))
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
