# Databricks notebook source
# notebook for cleaning the data
# used for books_rating file

# COMMAND ----------

from pyspark.sql.functions import when, col

# COMMAND ----------

# MAGIC %sql
# MAGIC --the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

# path for writing back to the storage the cleaned data
writing_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AN_Books/books_rating_silver"
)

# COMMAND ----------

# saving the data into a dataframe
# the col Book-Rating was full of 0 so I replaced them with null
df = (
    spark.readStream.table("books_rating_bronze")
    .withColumn("Book-Rating", col("Book-Rating").cast("Integer"))
    .withColumn(
        "Book-Rating", when(col("Book-Rating") == 0, None).otherwise(col("Book-Rating"))
    )
)

# COMMAND ----------

df.writeStream.format("delta").option(
    "checkpointLocation",
    "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/silver_ratings_checkpoint/",
).option("path", writing_path).outputMode("append").table("silver_ratings")
