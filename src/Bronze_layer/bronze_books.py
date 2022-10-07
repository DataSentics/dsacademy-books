# Databricks notebook source
# MAGIC %sql
# MAGIC USE andrei_tugmeanu_books

# COMMAND ----------

# MAGIC %run ../Autoloader

# COMMAND ----------

books_path = "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format(
    "andreitugmeanu"
)

# COMMAND ----------

data_loader = autoloader(
    books_path,
    "csv",
    "/dbfs/user/andrei-cosmin.tugmeanu@datasentics.com/dbacademy/books_checkpoint/",
    ";",
)

# COMMAND ----------

books_output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AT_books/Bronze/books"
)

# COMMAND ----------

data_loader.writeStream.format("delta").option(
    "checkpointLocation",
    "/dbfs/user/andrei-cosmin.tugmeanu@datasentics.com/dbacademy/books_checkpoint_new/",
).option("path", books_output_path).outputMode("append").table("bronze_books")
