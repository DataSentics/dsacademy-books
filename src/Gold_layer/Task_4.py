# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### The user's country who rated the most books published after the year 2000

# COMMAND ----------

new_df1 = (df1
            .select("Book_Rating","ISBN", "User_ID")
          )

new_df2 = (df2
          .select(["Year_Of_Publication", "ISBN"])
         )

new_df3 = (df3
          .select(["Country", "User_ID"])
         )

new_df = new_df1.join(new_df2, "ISBN",how="inner")

new_df = new_df.join(new_df3, "User_ID",how="inner")

new_df = (new_df
          .where(col("Book_Rating").isNotNull())
          .filter("Year_Of_Publication > 2000")
         )

new_df = (new_df
          .groupBy(["User_ID", "Country"])
          .count()
         )

new_df = (new_df
          .withColumnRenamed("count", "Number_of_ratings")
          .sort(col("Number_of_ratings").desc())
          .filter("Country != 'n/a'")
          .limit(1)
         )


display(new_df)
