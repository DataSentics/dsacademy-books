# Databricks notebook source
spark.sql("CREATE DATABASE IF NOT EXISTS daniela_vlasceanu_books")
spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

users_path = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('01rawdata') + 'books_crossing/BX-Users.csv'

df_users= (spark
           .read
           .option("sep", ";")
           .option("header", True)
           .option("inferSchema", True)
           .option("encoding", "ISO-8859-1")
           .csv(users_path)
            )

df_users.createOrReplaceTempView("users_bronze_tempView")
spark.sql(
    "CREATE OR REPLACE TABLE users_bronze AS SELECT * FROM users_bronze_tempView"
)

# COMMAND ----------

users_path_upload = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata') + 'daniela-vlasceanu-books/bronze/users'

df_users.write.parquet(users_path_upload, mode='overwrite')
