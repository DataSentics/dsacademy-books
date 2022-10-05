# Databricks notebook source
# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

# MAGIC %run ../AutoLoader

# COMMAND ----------

books_path = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/Users'.format('begalexandrunarcis')

# COMMAND ----------

Loading_data = auto_loader(
    books_path,
    "csv",
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/users_checkpoint/",
    ";",
)

# COMMAND ----------

users_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'AlexB_Books/bronze/users'
)

# COMMAND ----------

Loading_data.writeStream.option(
    "checkpointLocation",
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/users_checkpoint/",
).option("mergeSchema", "true").option("path", users_output_path).outputMode(
    "append"
).table(
    "bronze_users"
)
