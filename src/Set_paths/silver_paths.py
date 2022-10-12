# Databricks notebook source
# MAGIC %sql
# MAGIC USE andrei_tugmeanu_books

# COMMAND ----------

from pyspark.sql.functions import when, col, split

# COMMAND ----------

books_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AT_books/Bronze/books"
)

ratings_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AT_books/Bronze/books_ratings"
)

users_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AT_books/Bronze/books_users"
)

users_pii_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AT_books/Bronze/books_users_pii"
)

# COMMAND ----------

books_output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AT_books/Silver/books"
)

ratings_output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AT_books/Silver/books_ratings"
)

users_output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AT_books/Silver/users_ratings"
)

users_pii_output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AT_books/Silver/books_users_pii"
)

users_3nf_output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AT_books/Silver/books_users_3nf"
)

# COMMAND ----------

books_checkpoint = (
    "/dbfs/user/andrei-cosmin.tugmeanu@datasentics.com/dbacademy/silver_books_checkpoint/"
)

ratings_checkpoint = (
    "/dbfs/user/andrei-cosmin.tugmeanu@datasentics.com/dbacademy/silver_ratings_checkpoint/"
)

users_checkpoint = (
    "/dbfs/user/andrei-cosmin.tugmeanu@datasentics.com/dbacademy/silver_users_checkpoint/"
)

users_pii_checkpoint = (
    "/dbfs/user/andrei-cosmin.tugmeanu@datasentics.com/dbacademy/silver_users_pii_checkpoint/"
)

users_3nf_checkpoint = (
    "/dbfs/user/andrei-cosmin.tugmeanu@datasentics.com/dbacademy/silver_users_3nf_checkpoint/"
)
