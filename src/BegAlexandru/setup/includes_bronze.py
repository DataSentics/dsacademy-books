# Databricks notebook source
# This folder is dedicated for running the command use for the databse
# and for giving all the paths for the needed bronze parsing

# COMMAND ----------

# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

# bronze books
books_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/Books'.format('begalexandrunarcis')
)
books_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'BegAlex_Books/bronze/books'
)
checkpoint_books_path = (
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/books_checkpoint"
)
checkpoint_write_books_path = (
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/books_checkpoint_write"
)

# COMMAND ----------

# bronze ratings
ratings_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/Book-Rating'.format('begalexandrunarcis')
)
books_rating_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'BegAlex_Books/bronze/books_rating'
)
checkpoint_ratings_path = (
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/ratings_checkpoint/"
)
checkpoint_write_ratings_path = (
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/ratings_checkpoint_write/"
)

# COMMAND ----------

# bronze users
users_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/Users'.format('begalexandrunarcis')
)
users_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'BegAlex_Books/bronze/users'
)
checkpoint_users_path = (
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/users_checkpoint/"
)
checkpoint_write_users_path = (
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/users_checkpoint_write/"
)

# COMMAND ----------

# bronze users-pii
users_pii_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/Users_pii/".format("begalexandrunarcis")
)
users_pii_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'BegAlex_Books/bronze/pii'
)
checkpoint_users_pii_path = (
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/users_pii_checkpoint/"
)
checkpoint_write_users_pii_path = (
    "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/users_pii_checkpoint_write/"
)
