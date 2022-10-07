# Databricks notebook source
# MAGIC %sql
# MAGIC USE andrei_tugmeanu_books

# COMMAND ----------

# MAGIC %run ../Autoloader

# COMMAND ----------

rating_path = "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format(
    "andreitugmeanu"
)

# COMMAND ----------

data_loader = autoloader(
    rating_path,
    "csv",
    "/dbfs/user/andrei-cosmin.tugmeanu@datasentics.com/dbacademy/books_ratings_checkpoint234/",
    ";",
)

# COMMAND ----------

output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AT_books/Bronze/books_ratings"
)

# COMMAND ----------

data_loader.writeStream.format("delta").option(
    "checkpointLocation",
    "/dbfs/user/andrei-cosmin.tugmeanu@datasentics.com/dbacademy/books_checkpoint_new7/",
).option("path", output_path).outputMode("append").table("bronze_ratings")
