from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("books_pipeline").getOrCreate()

def autoload_to_table(data_source, table_name, checkpoint_directory,
                      source_format, encoding, output_path, separator=";"):
    if source_format == "csv":
        query = (spark.readStream
                      .format("cloudFiles")
                      .option("sep", separator)
                      .option("encoding", encoding)
                      .option("cloudFiles.format", source_format)
                      .option("cloudFiles.schemaLocation", checkpoint_directory)
                      .load(data_source)
                      .writeStream
                      .format("delta")
                      .option("checkpointLocation", checkpoint_directory)
                      .option("mergeSchema", "true")
                      .option("path", output_path)
                      .trigger(availableNow=True)
                      .outputMode("append")
                      .table(table_name))
    elif source_format == "json":
        query = (spark.readStream
                      .format("cloudFiles")
                      .option("cloudFiles.format", source_format)
                      .option("cloudFiles.schemaLocation", checkpoint_directory)
                      .load(data_source)
                      .writeStream
                      .format("delta")
                      .option("checkpointLocation", checkpoint_directory)
                      .option("mergeSchema", "true")
                      .option("path", output_path)
                      .trigger(availableNow=True)
                      .outputMode("append")
                      .table(table_name))
    return query