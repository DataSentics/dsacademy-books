# Databricks notebook source
#From which country is the user who rated the most books published after the year 2000?

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %sql
# MAGIC --the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

df = spark.sql("select * from ANiteanuBooks.users_rating_books")

# COMMAND ----------

df = (
    df.filter(col("Year-Of-Publication") > "2000")
    .groupBy("User-ID", "country")
    .count()
    .sort("count", ascending=False)
    .limit(1)
)
