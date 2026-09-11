from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import lit


builder = (
    SparkSession.builder
    .appName("UpdateProcessedFilesSchema")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()


processed_files_path = "data/bronze/processed_files"

processed_files_df = (
    spark.read.format("delta").load(processed_files_path)
)

updated_df = (
    processed_files_df.withColumn("error_message",lit(None).cast("string"))
)
(
    updated_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(processed_files_path)
)
print("Processed files schema updated successfully.")

spark.read \
    .format("delta") \
    .load(processed_files_path) \
    .printSchema()


spark.stop()