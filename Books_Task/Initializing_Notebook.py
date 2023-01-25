# Databricks notebook source
# Importing the necessary libraries

from pyspark.sql import functions as f
from pyspark.sql import types as t
import math
import scipy.stats as st


# Creating and using the database

spark.sql("CREATE DATABASE IF NOT EXISTS dbacademy_ovidiu_toma_datasentics_com_book_task_advanced")
spark.sql("USE dbacademy_ovidiu_toma_datasentics_com_book_task_advanced")


# General path

general_path = '@adapeuacadlakeg2dev.dfs.core.windows.net/gdc_academy_ovidiu_toma'


# Raw paths

raw_files = f"abfss://01rawdata{general_path}"

ratings_path = f"{raw_files}/Ratings_Raw"
books_path = f"{raw_files}/Books_Raw"
users_path = f"{raw_files}/Users_Raw"
users_pii_path = f"{raw_files}/Users_PII_Raw"

ratings_checkpoint_raw = f"{raw_files}/ratings_raw_checkpoint"
books_checkpoint_raw = f"{raw_files}/books_raw_checkpoint"
users_checkpoint_raw = f"{raw_files}/users_raw_checkpoint"
users_pii_checkpoint_raw = f"{raw_files}/users_pii_raw_checkpoint"


# Bronze paths

parsed_files = f"abfss://02parseddata{general_path}/Data_Engineering_Workflow"

ratings_bronze_path = f"{parsed_files}/ratings_bronze"
books_bronze_path = f"{parsed_files}/books_bronze"
users_bronze_path = f"{parsed_files}/users_bronze"
users_pii_bronze_path = f"{raw_files}/users_pii_bronze"

ratings_checkpoint_bronze = f"{parsed_files}/ratings_bronze_checkpoint"
books_checkpoint_bronze = f"{parsed_files}/books_bronze_checkpoint"
users_checkpoint_bronze = f"{parsed_files}/users_bronze_checkpoint"
users_pii_checkpoint_bronze = f"{raw_files}/users_pii_bronze_checkpoint"


# Silver paths

silver_files = f"abfss://03cleanseddata{general_path}/Data_Engineering_Workflow"

ratings_silver_path = f"{silver_files}/ratings_silver"
books_silver_path = f"{silver_files}/books_silver"
users_silver_path = f"{silver_files}/users_silver"
user_ratings_path = f"{silver_files}/user_ratings"
users_pii_silver_path = f"{raw_files}/users_pii_silver"

ratings_checkpoint_silver = f"{parsed_files}/ratings_silver_checkpoint"
books_checkpoint_silver = f"{parsed_files}/books_silver_checkpoint"
users_checkpoint_silver = f"{parsed_files}/users_silver_checkpoint"
users_pii_checkpoint_silver = f"{raw_files}/users_pii_silver_checkpoint"


# Gold path

gold_path = f"abfss://04golddata{general_path}/Data_Engineering_Workflow"


# Flake 8 pass


pass_df = [{'Column_1': '5'},
           {'Column_1': '10'}]

pass_df = spark.createDataFrame(pass_df)

test_pass = pass_df.agg(f.avg('Column_1'))

# COMMAND ----------

# Wilson Confidence Interval Function

def wilson_lower_bound(pos, n, confidence=0.95):
    """
    Function to provide lower bound of wilson score
    :param pos: No of positive ratings
    :param n: Total number of ratings
    :param confidence: Confidence interval, by default is 95 %
    :return: Wilson Lower bound score
    """
    if n == 0:
        return 0
    z = st.norm.ppf(1 - (1 - confidence) / 2)
    phat = 1.0 * pos / n
    x = (phat + z * z / (2 * n) - z * math.sqrt((phat * (1 - phat) + z * z / (4 * n)) / n)) / (1 + z * z / n)
    return float(x)

spark.udf.register('wilson_lower_bound_UDF', wilson_lower_bound, t.DoubleType())

udf_wilson = udf(wilson_lower_bound, t.DoubleType())

# COMMAND ----------

# Bayesian Credibile Interval Function

def bayesian_rating_products(n, confidence=0.95):
    """
    Function to calculate wilson score for N star rating system.
    :param n: Array having count of star ratings where ith index
    represent the votes for that category i.e. [3, 5, 6, 7, 10]
    here, there are 3 votes for 1-star rating,
    similarly 5 votes for 2-star rating.
    :param confidence: Confidence interval
    :return: Score
    """
    if sum(n) == 0:
        return 0
    K = len(n)
    z = st.norm.ppf(1 - (1 - confidence) / 2)
    N = sum(n)
    first_part = 0.0
    second_part = 0.0
    for k, n_k in enumerate(n):
        first_part += (k + 1) * (n[k] + 1) / (N + K)
        second_part += (k + 1) * (k + 1) * (n[k] + 1) / (N + K)
    score = first_part - z * math.sqrt((second_part - first_part * first_part) / (N + K + 1))
    return float(score)

spark.udf.register('bayesian_udf', bayesian_rating_products, t.DoubleType())

udf_bayesian = udf(bayesian_rating_products, t.DoubleType())

# COMMAND ----------

def autoload_to_table(data_source, table_name, checkpoint_directory, path, source_format='csv', delimiter=';'):
    if source_format == 'csv':
        query = (spark.readStream
                      .format("cloudFiles")
                      .option("cloudFiles.format", source_format)
                      .option("sep", delimiter)
                      .option("encoding", "latin1")
                      .option("header", True)
                      .option("cloudFiles.schemaLocation", checkpoint_directory)
                      .load(data_source)
                      .writeStream
                      .format("delta")
                      .option("checkpointLocation", checkpoint_directory)
                      .option("mergeSchema", "true")
                      .option("path", path)
                      .trigger(availableNow=True)
                      .outputMode("append")
                      .table(table_name))
    elif source_format == 'json':
        query = (spark.readStream
                      .format("cloudFiles")
                      .option("cloudFiles.format", source_format)
                      .option("cloudFiles.schemaLocation", checkpoint_directory)
                      .load(data_source)
                      .writeStream
                      .format("delta")
                      .option("checkpointLocation", checkpoint_directory)
                      .option("mergeSchema", "true")
                      .trigger(availableNow=True)
                      .option("path", path)
                      .outputMode("append")
                      .table(table_name))
    return query

# COMMAND ----------

def read_stream(from_table):
    display((spark.read.table(from_table)))
