# Databricks notebook source
spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

df_users = spark.table("users_silver")
df_users_pii = spark.table("users_pii_silver")

# COMMAND ----------

df_users_joined = df_users.join(df_users_pii, "User-ID")

# display(df_users_joined)

# COMMAND ----------

df_users_joined.createOrReplaceTempView("users_joined_pii_tempView")
spark.sql("CREATE OR REPLACE TABLE users_joined_pii_silver AS SELECT * FROM users_joined_pii_tempView")

# COMMAND ----------

users_path_upload_3nf = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "daniela-vlasceanu-books/3NF/users"
)
df_users_joined.write.parquet(users_path_upload_3nf, mode="overwrite")
