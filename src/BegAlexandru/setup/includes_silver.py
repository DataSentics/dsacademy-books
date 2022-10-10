# Databricks notebook source
# This folder is dedicated for running the command use for the databse
# and for giving all the paths for the needed silver data modeling

# COMMAND ----------

# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

# silver books
books_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "BegAlex_Books/bronze/books"
)
books_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('03cleanseddata')
    + 'BegAlex_Books/silver/books'
)
checkpoint_books_path = (
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/silver_books_checkpoint/"
)

# COMMAND ----------

# silver ratings
books_rating_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'BegAlex_Books/bronze/books_rating'
)
rating_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('03cleanseddata')
    + 'BegAlex_Books/silver/books_rating'
)
checkpoint_ratings_path = (
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/silver_ratings_checkpoint/"
)

# COMMAND ----------

# silver users
user_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'BegAlex_Books/bronze/users'
)
user_output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + 'BegAlex_Books/silver/users'
)
checkpoint_users_path = (
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/silver_users_checkpoint/"
)

# COMMAND ----------

# silver users-pii
pii_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "BegAlex_Books/bronze/pii"
)
pii_output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "BegAlex_Books/silver/pii"
)
checkpoint_userspii_path = (
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/silver_userspii_checkpoint/"
)

# COMMAND ----------

# 3nf
NF3_output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "BegAlex_Books/3NF/users_3nf"
)
checkpoint_3nf_path = (
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/nf3_checkpoint/"
)
