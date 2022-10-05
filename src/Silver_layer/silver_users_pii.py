# Databricks notebook source
# MAGIC %sql
# MAGIC USE andrei_tugmeanu_books

# COMMAND ----------

users_pii_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AT_books/Bronze/books_users_pii"
)

# COMMAND ----------

df_users_pii = (spark.read.parquet(users_pii_path))

# COMMAND ----------

display(df_users_pii)

# COMMAND ----------

df_users_pii = df_users_pii.withColumnRenamed("User-ID", "User_ID")

# COMMAND ----------

df_users_pii.write.mode('overwrite').saveAsTable("silver_books_users_pii")

# COMMAND ----------

output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AT_books/Silver/books_users_pii"
)

# COMMAND ----------

df_users_pii.write.parquet(output_path, mode='overwrite')
