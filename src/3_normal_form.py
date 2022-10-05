# Databricks notebook source
# MAGIC %sql
# MAGIC USE andrei_tugmeanu_books

# COMMAND ----------

users_pii_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AT_books/Silver/books_users_pii"
)

users_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AT_books/Silver/books_users"
)

# COMMAND ----------

df_users_pii = spark.read.parquet(users_pii_path)

df_users = spark.read.parquet(users_path)

# COMMAND ----------

new_users = df_users_pii.join(df_users, "User_ID")

# COMMAND ----------

new_users.write.mode("overwrite").saveAsTable("users_3nf")

# COMMAND ----------

output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AT_books/users_3nf"
)

# COMMAND ----------

new_users.write.parquet(output_path, mode='overwrite')
