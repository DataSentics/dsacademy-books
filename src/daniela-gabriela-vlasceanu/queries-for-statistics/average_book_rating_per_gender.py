# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE daniela_vlasceanu_books")

# COMMAND ----------

df = spark.table("users_ratings")

# COMMAND ----------

between = 10
df_result = (
    df.withColumn("Between", f.col("Age") - (f.col("Age") % 10))
    .withColumn(
        "Between",
        f.concat(f.col("Between") + 1, f.lit(" - "), f.col("Between") + between),
    )
    .groupBy("Between", "gender")
    .agg(f.avg("Book-Rating").alias("Average-Book-Rating"))
)

# COMMAND ----------

df_result.createOrReplaceTempView("average_book_rating_by_gender_age")

# COMMAND ----------

spark.sql("SELECT * FROM average_book_rating_by_gender_age").show()
