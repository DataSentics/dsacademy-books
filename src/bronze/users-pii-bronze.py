# Databricks notebook source
spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

users_pii_path_download = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('01rawdata') + 'books_crossing/users-pii.json'

df_users_pii=(spark
           .read
           .option("sep", ",")
           .option("header", True)
           .option("inferSchema", True)
           .json(users_pii_path_download)
            )

# COMMAND ----------

df_users_pii.createOrReplaceTempView("users_pii_bronze_tempView")
spark.sql(
    "CREATE OR REPLACE TABLE users_pii_bronze AS SELECT * FROM users_pii_bronze_tempView"
)

# COMMAND ----------

users_pii_path_upload = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata') + 'daniela-vlasceanu-books/bronze/users-pii'

df_users_pii.write.parquet(users_pii_path_upload, mode='overwrite')
