# Databricks notebook source
import os

# COMMAND ----------

# This folder is dedicated for running the command use for the databse
# and for giving all the paths for the needed silver data modeling

# COMMAND ----------

# MAGIC %sql
# MAGIC USE alexandru_beg_books

# COMMAND ----------

# input_var is where the data is coming from after parsing
input_parsed_path = 'abfss://02parseddata@adapeuacadlakeg2dev.dfs.core.windows.net/'
# output_var is where the data will be going after cleaning
output_cleansed_path = 'abfss://03cleanseddata@adapeuacadlakeg2dev.dfs.core.windows.net/'
# checkpoint_var is the checkpoint location in databricks
checkpoint_path = "/dbfs/user/alexandru-narcis.beg@datasentics.com/dbacademy/"

# COMMAND ----------

# silver books
books_path = os.path.join(input_parsed_path, 'BegAlex_Books/bronze/books')
books_output_path = os.path.join(output_cleansed_path, 'BegAlex_Books/silver/books')
checkpoint_books_path = os.path.join(checkpoint_path, "silver_books_checkpoint")

# COMMAND ----------

# silver ratings
books_rating_path = os.path.join(input_parsed_path, 'BegAlex_Books/bronze/books_rating')
rating_output_path = os.path.join(output_cleansed_path, 'BegAlex_Books/silver/books_rating')
checkpoint_ratings_path = os.path.join(checkpoint_path, "silver_ratings_checkpoint")

# COMMAND ----------

# silver users
user_path = os.path.join(input_parsed_path, 'BegAlex_Books/bronze/users')
user_output_path = os.path.join(output_cleansed_path, 'BegAlex_Books/silver/users')
checkpoint_users_path = os.path.join(checkpoint_path, "silver_users_checkpoint")

# COMMAND ----------

# silver users-pii
pii_path = os.path.join(input_parsed_path, 'BegAlex_Books/bronze/pii')
pii_output_path = os.path.join(output_cleansed_path, 'BegAlex_Books/silver/pii')
checkpoint_userspii_path = os.path.join(checkpoint_path, "silver_userspii_checkpoint")

# COMMAND ----------

# 3nf
NF3_output_path = os.path.join(output_cleansed_path, 'BegAlex_Books/3NF/users_3nf')
checkpoint_3nf_path = os.path.join(checkpoint_path, "nf3_checkpoint")
