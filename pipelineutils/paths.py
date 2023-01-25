# source data paths

az_path = "adapeuacadlakeg2dev.dfs.core.windows.net/gdc_academy_checiches_alexandru"
book_ratings_path = f"abfss://01rawdata@{az_path}/book_ratings"
books_path = f"abfss://01rawdata@{az_path}/books"
users_path = f"abfss://01rawdata@{az_path}/users"
users_pii_path = f"abfss://01rawdata@{az_path}/users_pii"

# parsed data paths

bronze_book_ratings_path = f"abfss://02parseddata@{az_path}/bronze/book_ratings_bronze"
bronze_books_path = f"abfss://02parseddata@{az_path}/bronze/books_bronze"
bronze_users_path = f"abfss://02parseddata@{az_path}/bronze/users_bronze"
bronze_users_pii_path = f"abfss://02parseddata@{az_path}/bronze/users_pii_bronze"

# checkpoints paths

checkpoint_path = "dbfs:/dbacademy/alexandru.checiches@datasentics.com/checkpoints"
bronze_book_ratings_checkpoint_path = f"{checkpoint_path}/bronze_book_ratings_checkpoint"
bronze_books_checkpoint_path = f"{checkpoint_path}/bronze_books_checkpoint"
bronze_users_checkpoint_path = f"{checkpoint_path}/bronze_users_checkpoint"
bronze_users_pii_checkpoint_path = f"{checkpoint_path}/bronze_users_pii_checkpoint"

# cleansed data paths

silver_book_ratings_path = f"abfss://03cleanseddata@{az_path}/silver/book_ratings_silver"
silver_books_path = f"abfss://03cleanseddata@{az_path}/silver/books_silver"
silver_users_path = f"abfss://03cleanseddata@{az_path}/silver/users_silver"
silver_users_with_pii_path = f"abfss://03cleanseddata@{az_path}/silver/users_with_pii_silver"

# business ready paths

avg_book_rating_gender_age_group_path = f"abfss://04golddata@{az_path}/gold/avg_book_rating_gender_age_group"
lost_publishers_path = f"abfss://04golddata@{az_path}/gold/lost_publishers"
top_10_authors_ratings_weighted_path = f"abfss://04golddata@{az_path}/gold/top_10_authors_ratings_weighted"
top_10_authors_titles_weighted_path = f"abfss://04golddata@{az_path}/gold/top_10_authors_titles_weighted"
top_10_rated_books_after_2000_path = f"abfss://04golddata@{az_path}/gold/top_10_rated_books_after_2000"
top_10_rated_books_agesexcountry_path = f"abfss://04golddata@{az_path}/gold/top_10_rated_books_agesexcountry"
top_10_rated_popular_books_path = f"abfss://04golddata@{az_path}/gold/top_10_rated_popular_books"
user_with_most_ratings_after_2000_path = f"abfss://04golddata@{az_path}/gold/user_with_most_ratings_after_2000"