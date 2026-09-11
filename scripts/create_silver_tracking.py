from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType
)

# --------------------------------------------------
# Spark Session
# --------------------------------------------------

builder = (
    SparkSession.builder
    .appName("CreateSilverTracking")
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

# --------------------------------------------------
# Path
# --------------------------------------------------

tracking_path = "data/silver/processed_bronze"

# --------------------------------------------------
# Empty Schema
# --------------------------------------------------

schema = StructType([
    StructField("source_file",StringType(),True),
    StructField("processed_at",TimestampType(),True),
    StructField("status",StringType(),True),
    StructField("error_message",StringType(),True)
]
)

# --------------------------------------------------
# Create Empty DataFrame
# --------------------------------------------------

tracking_df = spark.createDataFrame([],schema)

# --------------------------------------------------
# Write Delta Table
# --------------------------------------------------

(
    tracking_df.write.format("delta").mode("overwrite").save(tracking_path)
)

print("Silver tracking table created successfully.")

# --------------------------------------------------
# Verify
# --------------------------------------------------

spark.read.format("delta").load(tracking_path).printSchema()

spark.stop()


