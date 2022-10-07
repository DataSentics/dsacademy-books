# Databricks notebook source
# MAGIC %sql
# MAGIC USE andrei_tugmeanu_books

# COMMAND ----------

# MAGIC %run ../Autoloader

# COMMAND ----------

users_path = "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format(
    "andreitugmeanu"
)

# COMMAND ----------

data_loader = autoloader(
    users_path,
    "csv",
    "/dbfs/user/andrei-cosmin.tugmeanu@datasentics.com/dbacademy/books_users_checkpoint/",
    ";",
)

# COMMAND ----------

output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AT_books/Bronze/books_users"
)

# COMMAND ----------

data_loader.writeStream.format("delta").option(
    "checkpointLocation",
    "/dbfs/user/andrei-cosmin.tugmeanu@datasentics.com/dbacademy/books_checkpoint_new2/",
).option("path", output_path).outputMode("append").table("bronze_users")
