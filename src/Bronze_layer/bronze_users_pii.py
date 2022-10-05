# Databricks notebook source
# MAGIC %sql
# MAGIC USE andrei_tugmeanu_books

# COMMAND ----------

users_path_pii = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("01rawdata")
    + "books_crossing/users-pii.json"
)

# COMMAND ----------

df_users_pii = (
    spark.read.format("json")
    # .option("encoding", "ISO-8859-1")
    .option("header", "true")
    .option("sep", ";")
    .load(users_path_pii)
)

# COMMAND ----------

df_users_pii.write.mode('overwrite').saveAsTable("bronze_users_pii")

# COMMAND ----------

output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AT_books/Bronze/books_users_pii"
)

# COMMAND ----------

df_users_pii.write.parquet(output_path, mode='overwrite')
