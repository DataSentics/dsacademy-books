# Databricks notebook source
# Create Database if it does not exist

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS radomirfabian_books

# COMMAND ----------

# Use the created database

# COMMAND ----------

# MAGIC %sql
# MAGIC USE radomirfabian_books

# COMMAND ----------

# Paths

# COMMAND ----------

# bronze books
books_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/Books'.format('radomirfabian')
)
books_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'radomirfabian/bronze/books'
)
checkpoint_books_path = (
    "/dbfs/user/fabian-augustin-ilie.radomir@datasentics.com/dbacademy/books_checkpoint"
)
checkpoint_write_books_path = (
    "/dbfs/user/fabian-augustin-ilie.radomir@datasentics.com/dbacademy/books_checkpoint_write"
)

# COMMAND ----------

# bronze ratings
ratings_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/Book-Rating'.format('radomirfabian')
)
books_rating_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'radomirfabian/bronze/books_rating'
)
checkpoint_ratings_path = (
    "/dbfs/user/fabian-augustin-ilie.radomir@datasentics.com/dbacademy/ratings_checkpoint/"
)
checkpoint_write_ratings_path = (
    "/dbfs/user/fabian-augustin-ilie.radomir@datasentics.com/dbacademy/ratings_checkpoint_write/"
)

# COMMAND ----------

# bronze users
users_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/Users'.format('radomirfabian')
)
users_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'radomirfabian/bronze/users'
)
checkpoint_users_path = (
    "/dbfs/user/fabian-augustin-ilie.radomir@datasentics.com/dbacademy/users_checkpoint/"
)
checkpoint_write_users_path = (
    "/dbfs/user/fabian-augustin-ilie.radomir@datasentics.com/dbacademy/users_checkpoint_write/"
)

# COMMAND ----------

# bronze users-pii
users_pii_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/Users-Pii'.format('radomirfabian')
)
users_pii_output_path = (
    'abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/'.format('02parseddata')
    + 'radomirfabian/bronze/pii'
)
checkpoint_users_pii_path = (
    "/dbfs/user/fabian-augustin-ilie.radomir@datasentics.com/dbacademy/users_pii_checkpoint/"
)
checkpoint_write_users_pii_path = (
    "/dbfs/user/fabian-augustin-ilie.radomir@datasentics.com/dbacademy/users_pii_checkpoint_write/"
)
