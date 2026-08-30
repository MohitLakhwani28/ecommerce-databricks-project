from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("EcommerceBronzeIngestion")
    .master("local[*]")
    .getOrCreate()
)

input_path = "data/raw/orders.csv"
output_path = "data/bronze/orders"

orders_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

orders_df.show()
orders_df.printSchema()

orders_df.write.mode("overwrite").parquet(output_path)

spark.stop()