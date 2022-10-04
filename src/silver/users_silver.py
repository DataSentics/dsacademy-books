# Databricks notebook source
# notebook for cleaning the data
# used for user file

# COMMAND ----------

from pyspark.sql.functions import col, split

# COMMAND ----------

# MAGIC %sql
# MAGIC --the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

# path for reading the data in raw format
reading_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AN_Books/users"
)
# path for writing the data after cleaning it
writing_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AN_Books/users_silver"
)

# COMMAND ----------

# saving the data into a dataframe
df = spark.read.parquet(reading_path)

# COMMAND ----------

# splitting the col location into 3 different col and droping it after
# casting the col Age to integer(string before)
df = (
    df.withColumn("City", split(col("location"), ",").getItem(0))
    .withColumn("State", split(col("location"), ",").getItem(1))
    .withColumn("Country", split(col("location"), ",").getItem(2))
    .withColumn("Age", col("Age").cast("Integer"))
    .drop("location")
)

# COMMAND ----------

display(df)

# COMMAND ----------

# registering the table in the metastore
df.write.mode("overwrite").saveAsTable("users_silver")

# COMMAND ----------

# writing it to the storage
df.write.parquet(writing_path, mode='overwrite')
