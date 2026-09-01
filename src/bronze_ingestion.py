from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


# 1. Configure Spark
builder = (
    SparkSession.builder
    .appName("EcommerceBronzeIngestion")
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

# 2. Create SparkSession with Delta support
spark = configure_spark_with_delta_pip(builder).getOrCreate()


# 3. Define paths
input_path = "data/raw/orders.csv"
output_path = "data/bronze/orders"


# 4. Read raw CSV
orders_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)


# 5. Inspect data
orders_df.show()
orders_df.printSchema()


# 6. Write Bronze data as Delta
(
    orders_df.write
    .format("delta")
    .mode("overwrite")
    .save(output_path)
)


# 7. Stop Spark
spark.stop()