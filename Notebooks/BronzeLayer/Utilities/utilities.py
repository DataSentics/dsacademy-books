from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t
from delta.tables import DeltaTable

#This SparkSession is necessary, else we get an error 'spark' is not defined.
spark = SparkSession.builder.appName("denis_boboescu").getOrCreate()




### A way to access the storage ~ Might be useful ~

storage_account_name = 'adapeuacadlakeg2dev'

storage_account_access_key = 'wA432KewaHRxET7kpSgyAAL6/6u031XV+wA0x/3P3UGbJLxNPxA30VBHO8euadaQ/Idcl+vGujvd+AStK8VTHg=='

raw_blob_container = '01rawdata'

spark.conf.set(

  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",

  storage_account_access_key)



# Raw Data Path & Checkpoint directories for Auto Loader

raw_file_location = "wasbs://" + raw_blob_container + "@" + storage_account_name + ".blob.core.windows.net/academy_books_crossing"

# GENERAL_PATH = '@adapeuacadlakeg2dev.blob.core.windows.net/books_crossing'



# Data sources to use for the function below :

ratings_path = f"{raw_file_location}/ratings_raw"
books_path = f"{raw_file_location}/books_raw/"
users_path = f"{raw_file_location}/users_raw"
users_pii_path = f"{raw_file_location}/users_pii_raw"



# Paths and Checkpoints for Bronze Layer

parsed_blob_container = '02parseddata'

parsed_file_location = "wasbs://" + parsed_blob_container + "@" + storage_account_name + ".blob.core.windows.net/academy_books_crossing"


ratings_bronze_path = f"{parsed_file_location}/ratings_bronze"
books_bronze_path = f"{parsed_file_location}/books_bronze"
users_bronze_path = f"{parsed_file_location}/users_bronze"
users_pii_bronze_path = f"{parsed_file_location}/users_pii_bronze"

ratings_checkpoint_bronze = f"{parsed_file_location}/ratings_bronze_checkpoint"
books_checkpoint_bronze = f"{parsed_file_location}/books_bronze_checkpoint/"
users_checkpoint_bronze = f"{parsed_file_location}/users_bronze_checkpoint"
users_pii_checkpoint_bronze = f"{parsed_file_location}/users_pii_bronze_checkpoint"


#Paths and checkpoints for Silver Layer

cleansed_blob_container = '03cleanseddata'

cleansed_file_location = "wasbs://" + parsed_blob_container + "@" + storage_account_name + ".blob.core.windows.net/academy_books_crossing_workflow"

ratings_silver_path = f"{cleansed_file_location}/ratings_silver"
books_silver_path = f"{cleansed_file_location}/books_silver"
users_silver_path = f"{cleansed_file_location}/users_silver"
user_ratings_path = f"{cleansed_file_location}/user_ratings"
users_pii_silver_path = f"{cleansed_file_location}/users_pii_silver"

ratings_checkpoint_silver = f"{parsed_file_location}/ratings_silver_checkpoint"
books_checkpoint_silver = f"{parsed_file_location}/books_silver_checkpoint"
users_checkpoint_silver = f"{parsed_file_location}/users_silver_checkpoint"
users_pii_checkpoint_silver = f"{parsed_file_location}/users_pii_silver_checkpoint"


def write_in_blob_storage_csv(read_from, write_in, table_name, schema_location):
    (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", 'csv')
    .option("sep", ';')
    .option("encoding", "latin1")
    .option("header", True)
    .option("cloudFiles.schemaLocation",f"{schema_location}")
    .load(f"{read_from}")
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{schema_location}")
    .option("mergeSchema", "true")
    .option("path", f"{write_in}")
    .trigger(availableNow=True)
    .outputMode("append")
    .table(f"{table_name}"))

def write_in_blob_storage_json(read_from, write_in, table_name, schema_location)
    (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", 'json')
    .option("cloudFiles.schemaLocation",f"{schema_location}")
    .load(f"{read_from}")
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{schema_location}")
    .option("mergeSchema", "true")
    .option("path",f"{write_in}")
    .trigger(availableNow=True)
    .outputMode("append")
    .table(f"{table_name}"))





