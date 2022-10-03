# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### 10 most popular books in any selected (parameter period = 2years e.g.) period (last year, 2 years, ever)

# COMMAND ----------

new_df = (df2
          .select(["Book_Title","Publisher", "Book_Author", "Year_Of_Publication", "ISBN"])
         )

new_df2 = (df1
            .select("Book_Rating","ISBN")
          )

new_df = new_df.join(new_df2, "ISBN",how="inner")


left_border = 1980
right_border = 2000

new_df = (new_df
          #.filter(f" Year_Of_Publication between {left_border} and {right_border}")
          .filter((col("Year_Of_Publication") > left_border) & (col("Year_Of_Publication") < right_border))
          .groupBy("Book_Title")
          .count()
         )

new_df = (new_df
          .withColumnRenamed("count", "Top_10_books_by_rating")
          .sort(col("Top_10_books_by_rating").desc())
          .take(10)
         )

display(new_df)
