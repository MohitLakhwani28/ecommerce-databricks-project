from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType
)


builder = (
    SparkSession.builder
    .appName("CreateGoldTracking")
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


tracking_path = "data/gold/processed_silver"

schema =    StructType([StructField("source_file",StringType(),True),StructField("processed_at",TimestampType(),True),
                        StructField("status",StringType(),True),StructField("error_message",StringType(),True)
 ])

tracking_df = spark.createDataFrame([],schema)

(
    tracking_df.write.
    format("delta").mode("overwrite").save(tracking_path)
)

print("Gold tracking table created successfully.")

spark.read.format("delta").load(tracking_path).printSchema()

spark.stop()