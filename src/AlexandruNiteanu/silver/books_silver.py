# Databricks notebook source
# notebook for cleaning the data
# used for books file

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
    + "AN_Books/books_silver"
)

# COMMAND ----------

# saving the data into a dataframe
df = spark.readStream.table("bronze_books").withColumn(
    "Year-Of-Publication",
    when(col("Year-Of-Publication") == 0, None).otherwise(col("Year-Of-Publication")),
)

# COMMAND ----------

# the col Year-Of-Publication was full of 0 so I replaced them with null
df.writeStream.format("delta").option(
    "checkpointLocation",
    "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/books_silver_checkpoint/",
).option("path", writing_path).outputMode("append").table("books_silver")
