# Databricks notebook source
# paths for reading/writing the data, database I will use, paths for the checkpoints 

# COMMAND ----------

# MAGIC %sql
# MAGIC --the database I'm using
# MAGIC use ANiteanuBooks

# COMMAND ----------

# pii_users paths
pii_path_raw = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("01rawdata")
    + "books_crossing/"
)
pii_path_parsed = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AN_Books/users_pii"
)
pii_path_cleansed = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AN_Books/pii_users_silver"
)

pii_users_raw_checkpoint = "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/piiUsers_raw_checkpoint/"
pii_users_checkpoint = "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/silver_piiusers_checkpoint/"

# COMMAND ----------

# books paths
books_path_raw = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/Bx-Books/".format("alexandruniteanu")
)
books_path_parsed = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AN_Books/books"
)
books_path_cleansed = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AN_Books/books_silver"
)
books_checkpoint_raw = "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/books_raw_checkpoint/"
books_checkpoint = "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/books_silver_checkpoint/"

# COMMAND ----------

# book_ratings paths
books_rating_path_raw = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/BX-Book-Ratings/".format("alexandruniteanu")
)
books_rating_path_parsed = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AN_Books/books_rating"
)
books_rating_path_cleansed = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AN_Books/books_rating_silver"
)

books_rating_raw_checkpoint = "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/raw_books_rating_checkpoint/"
books_rating_checkpoint = "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/silver_ratings_checkpoint/"

# COMMAND ----------

# users paths
users_path_raw = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/Bx-Users/".format("alexandruniteanu")
)
users_path_parsed = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "AN_Books/users"
)
users_path_cleansed = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AN_Books/users_silver"
)
users_path_3nf = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "AN_Books/users_3nf"
)
users_raw_checkpoint = "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/raw_users_checkpoint/"
users_checkpoint = "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/silver_users_checkpoint/"
users_3nf_checkpoint = "/dbfs/user/alexandru.niteanu@datasentics.com/dbacademy/3NF_users_checkpoint/"
