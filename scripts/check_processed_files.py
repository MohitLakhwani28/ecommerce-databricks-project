from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import input_file_name

builder = (
    SparkSession.builder
    .appName("CheckProcessedFiles")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

raw_path = "data/raw/*.csv"
processed_files_path = "data/bronze/processed_files"

all_files_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(raw_path)
    .withColumn("source_file", input_file_name())
)

processed_files_df = (
    spark.read
    .format("delta")
    .load(processed_files_path)
)

print("Already processed files:")
processed_files_df.show()

print("Files found in raw:")
all_files_df.select("source_file").distinct().show(truncate=False)

new_files_df = (
    all_files_df
    .select("source_file")
    .distinct()
    .join(
        processed_files_df.select("source_file").distinct(),
        on="source_file",
        how="left_anti"
    )
)

print("NEW FILES:")
new_files_df.show(truncate=False)

spark.stop()