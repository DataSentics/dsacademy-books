# Databricks notebook source
# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

# MAGIC %run ../AutoLoader

# COMMAND ----------

users_path = 'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/Users'.format('begalexandrunarcis')

# COMMAND ----------

Loading_users = auto_loader(
    users_path,
    "csv",
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/users_checkpoint/",
    ";",
)

# COMMAND ----------

users_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'BegAlex_Books/bronze/users'
)

# COMMAND ----------

(
    Loading_users
    .writeStream
    .format("delta")
    .option("checkpointLocation",
            "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/users_checkpoint/")
    .option("path", users_output_path)
    .outputMode("append")
    .table("bronze_users")
)
