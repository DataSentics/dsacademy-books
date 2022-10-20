# Databricks notebook source
# MAGIC %run ../variables

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

books_df = spark.table("books_joined_silver").drop("_rescued_data")
users_df = spark.table("users_joined_pii_silver").drop("_rescued_data")

df = books_df.join(users_df, "User_ID")

# COMMAND ----------

upload_path = (
    f"{azure_storage}".format("03cleanseddata")
    + "daniela-vlasceanu-books/silver/users_ratings"
)

# COMMAND ----------

(
    df
    .write
    .format("delta")
    .mode("overwrite")
    .option("path", upload_path)
    .saveAsTable("users_ratings")
)
