# Databricks notebook source
# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

# MAGIC %run ../AutoLoader

# COMMAND ----------

books_path = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/Books'.format('begalexandrunarcis')

# COMMAND ----------

Loading_data = auto_loader(
    books_path,
    "csv",
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/books_checkpoint/",
    ";",
)

# COMMAND ----------

books_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'BegAlex_Books/bronze/books'
)

# COMMAND ----------

Loading_data.writeStream.format("delta").option(
    "checkpointLocation",
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/books_checkpoint1/",
).option("path", books_output_path).outputMode(
    "append"
).table(
    "bronze_books"
)
