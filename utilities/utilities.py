from pyspark.sql import SparkSession

# This SparkSession is necessary, else we get an error 'spark' is not defined.

spark = SparkSession.builder.appName("denis_boboescu").getOrCreate()

# A way to access the storage ~ Might be useful ~

storage_account_name = 'adapeuacadlakeg2dev'

storage_account_access_key = 'wA432KewaHRxET7kpSgyAAL6/6u031XV+wA0x/3P3UGbJLxNPxA30VBHO8euadaQ/Idcl+vGujvd+AStK8VTHg=='

raw_blob_container = '01rawdata'

spark.conf.set("fs.azure.account.key." + storage_account_name + ".blob.core.windows.net", storage_account_access_key)

raw_file_location = "wasbs://" + raw_blob_container + "@" + storage_account_name + \
    ".blob.core.windows.net/academy_books_crossing"

ratings_path = f"{raw_file_location}/ratings_raw"
books_path = f"{raw_file_location}/books_raw"
users_path = f"{raw_file_location}/users_raw"
users_pii_path = f"{raw_file_location}/users_pii_raw"

# Paths and Checkpoints for Bronze Layer

parsed_blob_container = '02parseddata'

parsed_file_location = "wasbs://" + parsed_blob_container + "@" + storage_account_name + \
    ".blob.core.windows.net/academy_books_crossing"

ratings_bronze_path = f"{parsed_file_location}/ratings_bronze"
books_bronze_path = f"{parsed_file_location}/books_bronze"
users_bronze_path = f"{parsed_file_location}/users_bronze"
users_pii_bronze_path = f"{parsed_file_location}/users_pii_bronze"

ratings_checkpoint_bronze = f"{parsed_file_location}/ratings_bronze_checkpoint"
books_checkpoint_bronze = f"{parsed_file_location}/books_bronze_checkpoint/"
users_checkpoint_bronze = f"{parsed_file_location}/users_bronze_checkpoint"
users_pii_checkpoint_bronze = f"{parsed_file_location}/users_pii_bronze_checkpoint"

# Paths and checkpoints for Silver Layer

cleansed_blob_container = '03cleanseddata'

cleansed_file_location = "wasbs://" + cleansed_blob_container + "@" + storage_account_name + \
    ".blob.core.windows.net/academy_books_crossing"

ratings_silver_path = f"{cleansed_file_location}/ratings_silver"
books_silver_path = f"{cleansed_file_location}/books_silver"
users_silver_path = f"{cleansed_file_location}/users_silver"
user_ratings_path = f"{cleansed_file_location}/user_ratings"
users_pii_silver_path = f"{cleansed_file_location}/users_pii_silver"

joins_path = f"{cleansed_file_location}/joins"

# Gold Path

gold_blob_container = '04golddata'

gold_path = "wasbs://" + gold_blob_container + "@" + storage_account_name + \
    ".blob.core.windows.net/academy_books_crossing/"

top_books_worldwide = f"{gold_path}/top_books_worldwide"
top_most_rated_per_country = f"{gold_path}/top_most_rated_per_country"
most_rated_per_agegroup = f"{gold_path}/most_rated_per_agegroup"
top_rated_per_gender = f"{gold_path}/top_rated_per_gender"
top_rated_authors = f"{gold_path}/top_rated_authors"

regex_pattern = "[^A-Za-z]"  # for Author Column

html_symbols = ["&amp;", "&lt;", "&gt;", "&quot;", "&apos;"]  # Add more symbols if needed

# display(books_silver.select('Publisher').where(f.col('Publisher').like('%&amp;%'))) # checking for HTML '&'