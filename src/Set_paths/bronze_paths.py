# Databricks notebook source
# MAGIC %sql
# MAGIC USE andrei_tugmeanu_books

# COMMAND ----------

books_path = "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/BX-Book".format(
    "andreitugmeanu"
)

rating_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/BX-Book-Ratings".format(
        "andreitugmeanu"
    )
)

users_path = "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/BX-Users".format(
    "andreitugmeanu"
)

users_pii_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/Users-pii/".format(
        "andreitugmeanu"
    )
)

# COMMAND ----------

books_checkpoint = (
    "/dbfs/user/andrei-cosmin.tugmeanu@datasentics.com/dbacademy/books_checkpoint/"
)

ratings_checkpoint = (
    "/dbfs/user/andrei-cosmin.tugmeanu@datasentics.com/dbacademy/ratings_checkpoint/"
)

users_checkpoint = (
    "/dbfs/user/andrei-cosmin.tugmeanu@datasentics.com/dbacademy/users_checkpoint/"
)

users_pii_checkpoint = (
    "/dbfs/user/andrei-cosmin.tugmeanu@datasentics.com/dbacademy/users_pii_checkpoint/"
)

# COMMAND ----------

books_output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AT_books/Bronze/books"
)

ratings_output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AT_books/Bronze/books_ratings"
)

users_output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AT_books/Bronze/books_users"
)

users_pii_output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AT_books/Bronze/books_users_pii"
)

# COMMAND ----------

books_write_path = "/dbfs/user/andrei-cosmin.tugmeanu@datasentics.com/dbacademy/books_checkpoint_write/"

ratings_write_path = "/dbfs/user/andrei-cosmin.tugmeanu@datasentics.com/dbacademy/ratings_checkpoint_write/"

users_write_path = "/dbfs/user/andrei-cosmin.tugmeanu@datasentics.com/dbacademy/users_checkpoint_write/"

users_pii_write_path = "/dbfs/user/andrei-cosmin.tugmeanu@datasentics.com/dbacademy/users_pii_checkpoint_write/"
