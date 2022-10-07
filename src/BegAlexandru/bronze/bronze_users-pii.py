# Databricks notebook source
# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

# MAGIC %run ../AutoLoader

# COMMAND ----------

users_pii_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("begalexandrunarcis")
    + "Users_pii/"
)

# COMMAND ----------

Loading_userspii = auto_loader(
    users_pii_path,
    "json",
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/users_pii_checkpoint_new/",
    ",",
)

# COMMAND ----------

users_pii_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'BegAlex_Books/bronze/pii'
)

# COMMAND ----------

(
    Loading_userspii
    .writeStream
    .format("delta")
    .option("checkpointLocation",
            "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/users_pii_checkpoint1_new/")
    .option("path", users_pii_output_path)
    .outputMode("append")
    .table("bronze_pii")
)
