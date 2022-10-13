# Databricks notebook source
# This folder is dedicated for running the command use for the databse
# and for giving all the paths for the needed silver data modeling

# COMMAND ----------

# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

# input_var is where the data is coming from after parsing
input_var = 'abfss://02parseddata@adapeuacadlakeg2dev.dfs.core.windows.net/'
# output_var is where the data will be going after cleaning
output_var = 'abfss://03cleanseddata@adapeuacadlakeg2dev.dfs.core.windows.net/'
# checkpoint_var is the checkpoint location in databricks
checkpoint_var = "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/"

# COMMAND ----------

# silver books
books_path = (
    input_var + "BegAlex_Books/bronze/books"
)
books_output_path = (
    output_var + 'BegAlex_Books/silver/books'
)
checkpoint_books_path = (
    checkpoint_var + "silver_books_checkpoint/"
)

# COMMAND ----------

# silver ratings
books_rating_path = (
    input_var + 'BegAlex_Books/bronze/books_rating'
)
rating_output_path = (
    output_var + 'BegAlex_Books/silver/books_rating'
)
checkpoint_ratings_path = (
    checkpoint_var + "silver_ratings_checkpoint/"
)

# COMMAND ----------

# silver users
user_path = (
    input_var + 'BegAlex_Books/bronze/users'
)
user_output_path = (
    output_var + 'BegAlex_Books/silver/users'
)
checkpoint_users_path = (
    checkpoint_var + "silver_users_checkpoint/"
)

# COMMAND ----------

# silver users-pii
pii_path = (
    input_var + "BegAlex_Books/bronze/pii"
)
pii_output_path = (
    output_var + "BegAlex_Books/silver/pii"
)
checkpoint_userspii_path = (
    checkpoint_var + "silver_userspii_checkpoint/"
)

# COMMAND ----------

# 3nf
NF3_output_path = (
    output_var + "BegAlex_Books/3NF/users_3nf"
)
checkpoint_3nf_path = (
    checkpoint_var + "nf3_checkpoint/"
)
