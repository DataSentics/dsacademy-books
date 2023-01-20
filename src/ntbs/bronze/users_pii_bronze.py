# Databricks notebook source
# MAGIC %run ../setup/initial_book_setup

# COMMAND ----------

import pipelineutils.pathz as P
import pipelineutils.autoloader as A

# COMMAND ----------

A.autoload_to_table(P.users_pii_path,
                    "users_pii_bronze",
                    P.bronze_users_pii_checkpoint_path,
                    "json", "latin1",
                    P.bronze_users_pii_path,
                    ";"
                    )
