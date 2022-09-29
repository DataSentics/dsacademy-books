# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

#paths for raw data
book_ratings_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("begalexandrunarcis")
    + "BX-Book-Ratings.csv"
)
books_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("begalexandrunarcis")
    + "BX-Books.csv"
)
users_path = "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/Users_directory/".format("begalexandrunarcis") 

# COMMAND ----------

schema_users = StructType(
    [
        StructField("User-ID", StringType()),
        StructField("Location", StringType()),
        StructField("Age", StringType()),
    ]
)
(
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation","dbfs:/mnt/dbacademy-users/alexandru-narcis.beg@datasentics.com/checkpoints")
    .load(users_path)
    .writeStream.option("checkpointLocation", "dbfs:/mnt/dbacademy-users/alexandru-narcis.beg@datasentics.com/checkpoints")
    .option("mergeSchema", "true")
    .table("Test_ffw")
)

# COMMAND ----------

df_books = (
    spark.read.option("encoding", "ISO-8859-1").option("header", "true").option("delimiter", ";").csv(books_path)
)

# COMMAND ----------

df_books.createOrReplaceTempView("TestView")

# COMMAND ----------

books_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'BegAlex_Books/bronze/books_test_2'
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE test_2_bronze AS SELECT * FROM TestView LOCATION ${books_output_path}

# COMMAND ----------


