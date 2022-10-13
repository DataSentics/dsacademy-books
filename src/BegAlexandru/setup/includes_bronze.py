# Databricks notebook source
# This folder is dedicated for running the command use for the databse
# and for giving all the paths for the needed bronze parsing

# COMMAND ----------

# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

# input_var is where the data is coming originally from
input_var = 'abfss://begalexandrunarcis@adapeuacadlakeg2dev.dfs.core.windows.net/'
# output_var is where the data will be going after parsing
output_var = 'abfss://02parseddata@adapeuacadlakeg2dev.dfs.core.windows.net/'
# checkpoint_var is the checkpoint location in databricks
checkpoint_var = "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/"

# COMMAND ----------

# bronze books
books_path = (
    input_var + 'Books'
)
books_output_path = (
    output_var + 'BegAlex_Books/bronze/books'
)
checkpoint_books_path = (
    checkpoint_var + "books_checkpoint"
)
checkpoint_write_books_path = (
    checkpoint_var + "books_checkpoint_write"
)

# COMMAND ----------

# bronze ratings
ratings_path = (
    input_var + 'Book-Rating'
)
books_rating_output_path = (
    output_var + 'BegAlex_Books/bronze/books_rating'
)
checkpoint_ratings_path = (
    checkpoint_var + "ratings_checkpoint/"
)
checkpoint_write_ratings_path = (
    checkpoint_var + "ratings_checkpoint_write/"
)

# COMMAND ----------

# bronze users
users_path = (
    input_var + 'Users'
)
users_output_path = (
    output_var + 'BegAlex_Books/bronze/users'
)
checkpoint_users_path = (
    checkpoint_var + "users_checkpoint/"
)
checkpoint_write_users_path = (
    checkpoint_var + "users_checkpoint_write/"
)

# COMMAND ----------

# bronze users-pii
users_pii_path = (
    input_var + 'Users_pii/'
)
users_pii_output_path = (
    output_var + 'BegAlex_Books/bronze/pii'
)
checkpoint_users_pii_path = (
    checkpoint_var + "users_pii_checkpoint/"
)
checkpoint_write_users_pii_path = (
    checkpoint_var +"users_pii_checkpoint_write/"
)
