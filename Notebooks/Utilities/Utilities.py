from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t
from delta.tables import DeltaTable

#This SparkSession is necessary, else we get an error 'spark' is not defined.
spark = SparkSession.builder.appName("denis_boboescu").getOrCreate()




### A way to access the storage ~ Might be useful ~

storage_account_name = 'adapeuacadlakeg2dev'

storage_account_access_key = 'wA432KewaHRxET7kpSgyAAL6/6u031XV+wA0x/3P3UGbJLxNPxA30VBHO8euadaQ/Idcl+vGujvd+AStK8VTHg=='

blob_container = '01rawdata'

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_access_key)




# Raw Data Path & Checkpoint directories for Auto Loader


GENERAL_PATH = '@adapeuacadlakeg2dev.blob.core.windows.net/books_crossing'


RAW_FILES = f"abfss://01rawdata{GENERAL_PATH}"

# Data sources to use for the function below :

ratings_path = f"{RAW_FILES}/ratings_raw"
books_path = f"{RAW_FILES}/books_raw"
users_path = f"{RAW_FILES}/users_raw"
users_pii_path = f"{RAW_FILES}/users_pii_raw"

ratings_checkpoint_raw = f"{RAW_FILES}/ratings_raw_checkpoint"
books_checkpoint_raw = f"{RAW_FILES}/books_raw_checkpoint"
users_checkpoint_raw = f"{RAW_FILES}/users_raw_checkpoint"
users_pii_checkpoint_raw = f"{RAW_FILES}/users_pii_raw_checkpoint"

print(books_checkpoint_raw)
# Should we use a function? 

source_format='csv'

delimiter =';'

def autoloader(data_source, table_name, path, source_format, delimiter):
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", source_format)
                  .option("sep", delimiter)
                  .option("encoding", "latin1")
                  .option("header", True)
                  .load(data_source)
                  .writeStream
                  .format("delta")
                  .option("path", path)
                  .trigger(availableNow=True)
                  .outputMode("append")
                  .table(table_name))
    
    return query

# A way to use the autoloader function 

#ratings_table = autoloader(ratings_path, "ratings_table", ratings_checkpoint_raw, source_format, delimiter)
#books_table = autoloader(books_path, "books_table", books_checkpoint_raw, source_format, delimiter)
#users_table = autoloader(users_path, "users_table", users_checkpoint_raw, source_format, delimiter)
#users_pii_table = autoloader(users_pii_path, "users_pii_table", users_pii_checkpoint_raw, source_format, delimiter)

