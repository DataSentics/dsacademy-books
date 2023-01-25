# Databricks notebook source
# MAGIC %run ../initial_notebook

# COMMAND ----------

from pyspark.sql.functions import col, count


def lost_publishers(df, from_year):
    df_truncated = df.where(col('Year-Of-Publication') >= from_year)
    return (df.exceptAll(df_truncated)
            .select('Publisher', 'Year-Of-Publication')
            .groupBy(['Publisher', 'Year-Of-Publication']).agg(count('Publisher').alias("Nr-Of-Publications")))

books_silver_df = spark.table("books_silver")

df = lost_publishers(books_silver_df, 1924).sort('Year-Of-Publication', ascending=[False])

(df.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .option("path", f'{answer_question}/lost_publishers')
 .saveAsTable("lost_publishers_answer"))

# COMMAND ----------

display(spark.table('lost_publishers_answer'))
