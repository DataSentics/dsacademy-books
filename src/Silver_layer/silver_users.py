# Databricks notebook source
from pyspark.sql.functions import col, split

# COMMAND ----------

# MAGIC %sql
# MAGIC USE andrei_tugmeanu_books

# COMMAND ----------

books_users_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AT_books/Bronze/books_users"
)

# COMMAND ----------

df_books_users = (spark.read.parquet(books_users_path))

# COMMAND ----------

df_books_users = (
    spark.read.parquet(books_users_path)
    .withColumnRenamed("User-ID", "User_ID")
    .withColumn("Age", when(col("Age") == "NULL", "Unknown").otherwise(col("Age")))
    .withColumn("City", split(col("Location"), ",").getItem(0))
    .withColumn("State", split(col("Location"), ",").getItem(1))
    .withColumn("Country", split(col("Location"), ",").getItem(2))
    .drop("Location")
    .withColumn("State", when(col("State") == " ", "Unknown").otherwise(col("State")))
    .withColumn(
        "Country", when(col("Country") == "", "Unknown").otherwise(col("Country"))
    )
    .withColumn("City", when(col("City") == " ", "Unknown").otherwise(col("City")))
)


# COMMAND ----------

df_books_users.write.mode('overwrite').saveAsTable("silver_books_users")

# COMMAND ----------

output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AT_books/Silver/books_users"
)

# COMMAND ----------

df_books_users.write.parquet(output_path, mode='overwrite')

# COMMAND ----------


