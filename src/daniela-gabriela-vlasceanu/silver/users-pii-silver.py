# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

df_users_pii = spark.readStream.table("users_pii_bronze")

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
    .drop(f.col("_rescued_data"))
)

# COMMAND ----------

users_pii_path_upload_2 = (
    "abfss://{}@adapeuacadlakeg2dev.dfs.core.windows.net/".format("03cleanseddata")
    + "daniela-vlasceanu-books/silver/users_pii"
)

# COMMAND ----------

df_users_pii_cleansed.createOrReplaceTempView("users_pii_silver_tempView")

# COMMAND ----------

spark.table("users_pii_silver_tempView").writeStream.trigger(availableNow=True).format("delta").option(
    "checkpointLocation",
    "/dbfs/user/daniela-gabriela.vlasceanu@datasentics.com/dbacademy/daniela_users_pii_silver_checkpoint/",
).option("path", users_pii_path_upload_2).outputMode(
    "append"
).table(
    "users_pii_silver"
)
