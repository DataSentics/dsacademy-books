# Databricks notebook source
from pyspark.sql.functions import when, col

# COMMAND ----------

# MAGIC %sql
# MAGIC use alexandru_beg_books

# COMMAND ----------

books_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "BegAlex_Books/bronze/books"
)

# COMMAND ----------

# cleaning and reading the data from bronze books
books_df = (
    spark.readStream
    .table("bronze_books")
    .withColumn("Year-Of-Publication",
        when(col("Year-Of-Publication") == "0", "unknown")
                .otherwise(col("Year-Of-Publication")),
    )
    .fillna("unknown")
)

# COMMAND ----------

books_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('03cleanseddata')
    + 'BegAlex_Books/silver/books'
)

# COMMAND ----------

(
    books_df
    .writeStream
    .format("delta").option("checkpointLocation",
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/silver_books_checkpoint1/")
    .option("path", books_output_path)
    .outputMode("append")
    .table("silver_books")
)
