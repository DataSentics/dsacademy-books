# Databricks notebook source
import os

# COMMAND ----------

# This folder is dedicated for running the command use for the databse
# and for giving all the paths for the needed bronze parsing

# COMMAND ----------

# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

# input_var is where the data is coming originally from
raw_lake_path = 'abfss://begalexandrunarcis@adapeuacadlakeg2dev.dfs.core.windows.net/'
# output_var is where the data will be going after parsing
parsed_lake_path = 'abfss://02parseddata@adapeuacadlakeg2dev.dfs.core.windows.net/'
# checkpoint_var is the checkpoint location in databricks
checkpoint_path = "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/"

# COMMAND ----------

# bronze books
books_path = os.path.join(raw_lake_path, 'Books')
books_output_path = os.path.join(parsed_lake_path, 'BegAlex_Books/bronze/books')
checkpoint_books_path = os.path.join(checkpoint_path, "books_checkpoint")
checkpoint_write_books_path = os.path.join(checkpoint_path, "books_checkpoint_write")

# COMMAND ----------

# bronze ratings
ratings_path = os.path.join(raw_lake_path, 'Book-Rating')
books_rating_output_path = os.path.join(parsed_lake_path, 'BegAlex_Books/bronze/books_rating')
checkpoint_ratings_path = os.path.join(checkpoint_path, "ratings_checkpoint")
checkpoint_write_ratings_path = os.path.join(checkpoint_path, "ratings_checkpoint_write")

# COMMAND ----------

# bronze users
users_path = os.path.join(raw_lake_path, 'Users')
users_output_path = os.path.join(parsed_lake_path, 'BegAlex_Books/bronze/users')
checkpoint_users_path = os.path.join(checkpoint_path, "users_checkpoint")
checkpoint_write_users_path = os.path.join(checkpoint_path, "users_checkpoint_write")

# COMMAND ----------

# bronze users-pii
users_pii_path = os.path.join(raw_lake_path, 'Users_pii')
users_pii_output_path = os.path.join(parsed_lake_path, 'BegAlex_Books/bronze/pii')
checkpoint_users_pii_path = os.path.join(checkpoint_path, "users_pii_checkpoint")
checkpoint_write_users_pii_path = os.path.join(checkpoint_path, "users_pii_checkpoint_write")
