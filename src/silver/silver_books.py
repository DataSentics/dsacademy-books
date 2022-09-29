# Databricks notebook source
from pyspark.sql.functions import when

# COMMAND ----------

# MAGIC %sql
# MAGIC use alexandru_beg_books

# COMMAND ----------

#cleaning the data from bronze books

# COMMAND ----------

books_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "BegAlex_Books/bronze/books"
)

# COMMAND ----------

books_df = (
    spark.read.parquet(books_path)
    .withColumn(
        "Year-Of-Publication",
        when(col("Year-Of-Publication") == "0", "unknown").otherwise(
            col("Year-Of-Publication")
        ),
    )
    .fillna("unknown")
)


# COMMAND ----------

books_df.write.mode('overwrite').saveAsTable("silver_books")

# COMMAND ----------

books_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('03cleanseddata') 
    + 'BegAlex_Books/silver/books'
)

# COMMAND ----------

books_df.write.parquet(books_output_path, mode='overwrite')
