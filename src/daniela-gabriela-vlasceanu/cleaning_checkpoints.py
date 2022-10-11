# Databricks notebook source
dbutils.fs.rm(
    "/dbfs/user/daniela-gabriela.vlasceanu@datasentics.com/dbacademy/daniela_books_raw_checkpoint/",
    True,
)

# COMMAND ----------

dbutils.fs.rm(
    "/dbfs/user/daniela-gabriela.vlasceanu@datasentics.com/dbacademy/daniela_books_ratings_raw_checkpoint/",
    True,
)

# COMMAND ----------

dbutils.fs.rm(
    "/dbfs/user/daniela-gabriela.vlasceanu@datasentics.com/dbacademy/daniela_users_raw_checkpoint/",
    True,
)

# COMMAND ----------

dbutils.fs.rm(
    "/dbfs/user/daniela-gabriela.vlasceanu@datasentics.com/dbacademy/daniela_users_pii_r_checkpoint/",
    True,
)

# COMMAND ----------

dbutils.fs.rm(
    "/dbfs/user/daniela-gabriela.vlasceanu@datasentics.com/dbacademy/daniela_users_joined_pii_checkpoint/",
    True,
)

# COMMAND ----------

dbutils.fs.rm(
    "/dbfs/user/daniela-gabriela.vlasceanu@datasentics.com/dbacademy/daniela_books_ratings_silver_checkpoint/",
    True,
)

# COMMAND ----------

dbutils.fs.rm(
    "/dbfs/user/daniela-gabriela.vlasceanu@datasentics.com/dbacademy/daniela_books_silver_checkpoint/",
    True,
)

# COMMAND ----------

dbutils.fs.rm(
    "/dbfs/user/daniela-gabriela.vlasceanu@datasentics.com/dbacademy/daniela_users_pii_silver_checkpoint/",
    True,
)

# COMMAND ----------

dbutils.fs.rm(
    "/dbfs/user/daniela-gabriela.vlasceanu@datasentics.com/dbacademy/daniela_users_silver_checkpoint/",
    True,
)
