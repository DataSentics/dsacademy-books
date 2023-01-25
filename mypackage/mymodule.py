import os
from pyspark.sql import SparkSession

# Create a new SparkSession object
spark = SparkSession.builder.getOrCreate()

az_path = '@adapeuacadlakeg2dev.dfs.core.windows.net/filip_megiesan'

raw_az_path = f'abfss://01rawdata{az_path}'

raw_book_ratings_path = os.path.join(raw_az_path, 'book_ratings')
raw_books_path = os.path.join(raw_az_path, 'books/')
raw_users_path = os.path.join(raw_az_path, 'users/')
raw_users_pii_path = os.path.join(raw_az_path, 'users_pii/')

bronze_az_path = f'abfss://02parseddata{az_path}'

bronze_book_ratings_path = os.path.join(bronze_az_path, 'book_ratings')
bronze_books_path = os.path.join(bronze_az_path, 'books')
bronze_users_path = os.path.join(bronze_az_path, 'users')
bronze_users_pii_path = os.path.join(bronze_az_path, 'users_pii')

silver_az_path = f'abfss://03cleanseddata{az_path}'

silver_book_ratings_path = os.path.join(silver_az_path, 'book_ratings')
silver_books_path = os.path.join(silver_az_path, 'books')
silver_users_path = os.path.join(silver_az_path, 'users')

gold_az_path = f'abfss://04golddata{az_path}'

gold_top10_authors_path = os.path.join(gold_az_path, 'top10_authors')
gold_lost_publishers_path = os.path.join(gold_az_path, 'lost_publishers')
gold_top10_books_period_path = os.path.join(gold_az_path, 'top10_books_period')
gold_country_of_top_user_path = os.path.join(gold_az_path, 'country_of_top_user')
gold_top10_books_AGC_path = os.path.join(gold_az_path, 'top10_books_AGC')
gold_top10_authors_AGC_path = os.path.join(gold_az_path, 'top10_authors_AGC')
gold_avg_book_rating_GA_path = os.path.join(gold_az_path, 'avg_book_rating_GA')

checkpoint_bronze_book_ratings = os.path.join(bronze_az_path, 'checkpoint_bronze_book_ratings')
checkpoint_bronze_books = os.path.join(bronze_az_path, 'checkpoint_bronze_books')
checkpoint_bronze_users = os.path.join(bronze_az_path, 'checkpoint_bronze_users')
checkpoint_bronze_users_pii = os.path.join(bronze_az_path, 'checkpoint_bronze_users_pii')