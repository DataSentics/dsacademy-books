# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

df_users = spark.table("users_bronze")

df_users_cleansed = (df_users
          .withColumn("location_city", f.split(f.col("Location"),",").getItem(0))
          .withColumn("location_state", f.split(f.col("Location"),",").getItem(1))
          .withColumn("location_country", f.split(f.col("Location"),",").getItem(2))
          .drop(f.col("Location"))
          .na.replace("N/a",None)
          .na.replace(" ",None)   
          .na.replace("",None)          
          .withColumn("Age", (f.when(f.col("Age") == "NULL",None).otherwise(f.col("Age"))).cast('int'))
          .withColumn("location_city",f.initcap(f.col("location_city")))
          .withColumn("location_state",f.initcap(f.col("location_state")))
          .withColumn("location_country",f.initcap(f.col("location_country")))
          .withColumn("User-ID", f.col("User-Id").cast("bigint"))
          )
display(df_users_cleansed)

# COMMAND ----------

users_path_upload_2 = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('03cleanseddata') + 'daniela-vlasceanu-books/silver/users'

df_users_cleansed.write.parquet(users_path_upload_2, mode='overwrite')

# COMMAND ----------

df_users_cleansed.createOrReplaceTempView("users_silver_tempView")
spark.sql(
    "CREATE OR REPLACE TABLE users_silver AS SELECT * FROM users_silver_tempView"
)
