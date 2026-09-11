from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("CreateProcessedFilesTable")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

processed_files_path = "data/bronze/processed_files"

processed_files_df = spark.createDataFrame(
    [],
    "source_file STRING,processed_at TIMESTAMP,status STRING"
)

processed_files_df.write.format("delta").mode("overwrite").save(processed_files_path)


print("Processed files table created successfully.")

spark.stop()