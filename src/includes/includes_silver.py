# Databricks notebook source
# This include file will set the corresponding database and
# Assign necessary variables


# COMMAND ----------

# MAGIC %sql
# MAGIC USE radomirfabian_books

# COMMAND ----------

# silver books
books_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "radomirfabian/bronze/books"
)
books_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('03cleanseddata')
    + 'radomirfabian/silver/books'
)
checkpoint_books_path = (
    "/dbfs/user/fabian-augustin-ilie.radomir@datasentics.com/dbacademy/silver_books_checkpoint/"
)

# COMMAND ----------

# silver book ratings
books_ratings_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "radomirfabian/bronze/books_rating"
)
ratings_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('03cleanseddata')
    + 'radomirfabian/silver/books_rating'
)
checkpoint_ratings_path = (
    "/dbfs/user/fabian-augustin-ilie.radomir@datasentics.com/dbacademy/silver_books_checkpoint/"
)

# COMMAND ----------

# silver book ratings
users_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "radomirfabian/bronze/users"
)
users_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('03cleanseddata')
    + 'radomirfabian/silver/users'
)
checkpoint_users_path = (
    "/dbfs/user/fabian-augustin-ilie.radomir@datasentics.com/dbacademy/silver_users_checkpoint/"
)

# COMMAND ----------

# silver book ratings
pii_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("02parseddata")
    + "radomirfabian/bronze/pii"
)
pii_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('03cleanseddata')
    + 'radomirfabian/silver/pii'
)
checkpoint_pii_path = (
    "/dbfs/user/fabian-augustin-ilie.radomir@datasentics.com/dbacademy/silver_userspii_checkpoint/"
)

# COMMAND ----------

# 3nf
normalizedform_output_path = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "radomirfabian_books/3NF/users_3nf"
)
checkpoint_3nf_path = (
    "/dbfs/user/fabian-augustin-ilie.radomir@datasentics.com/nf3_checkpoint/"
)
