# Databricks notebook source
# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

# MAGIC %run ../AutoLoader

# COMMAND ----------

ratings_path = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/Book-Rating'.format('begalexandrunarcis')

# COMMAND ----------

Loading_ratings = auto_loader(
    ratings_path,
    "csv",
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/ratings_checkpoint_new/",
    ";",
)

# COMMAND ----------

books_rating_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'BegAlex_Books/bronze/books_rating'
)

# COMMAND ----------

(
    Loading_ratings
    .writeStream
    .format("delta")
    .option("checkpointLocation",
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/ratings_checkpoint1_new/")
    .option("path", books_rating_output_path)
    .outputMode("append")
    .table("bronze_ratings")
)
