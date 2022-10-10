# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

df_users_pii = spark.table("users_pii_bronze")

df_users_pii_cleansed = (
    df_users_pii.withColumn(
        "FullName",
        f.concat(
            f.col("firstName"),
            f.lit(" "),
            f.col("middleName"),
            f.lit(" "),
            f.col("lastName"),
        ),
    )
    .drop(f.col("firstName"))
    .drop(f.col("middleName"))
    .drop(f.col("lastName"))
)
# display(df_users_pii_cleansed)

# COMMAND ----------

users_pii_path_upload_2 = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "daniela-vlasceanu-books/silver/users_pii"
)
df_users_pii_cleansed.write.parquet(users_pii_path_upload_2, mode="overwrite")

# COMMAND ----------

df_users_pii_cleansed.createOrReplaceTempView("users_pii_silver_tempView")
spark.sql(
    "CREATE OR REPLACE TABLE users_pii_silver AS SELECT * FROM users_pii_silver_tempView"
)

# COMMAND ----------

# %sql
# SELECT count(*) From users_pii_silver

# COMMAND ----------

# %sql
# SELECT count(DISTINCT (*)) FROM users_pii_silver
