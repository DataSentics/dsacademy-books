# Databricks notebook source
# MAGIC %run ../setup/initial_book_setup

# COMMAND ----------

import pipelineutils.pathz as P

# COMMAND ----------

autoload_to_table(P.users_path,
                  "users_bronze",
                  P.bronze_users_checkpoint_path,
                  "csv",
                  "latin1",
                  P.bronze_users_path,
                  ";")
