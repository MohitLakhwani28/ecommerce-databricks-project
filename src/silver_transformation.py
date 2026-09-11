from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, to_date


# 1. Configure Spark
builder = (
    SparkSession.builder
    .appName("EcommerceSilverTransformation")
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

bronze_path = "data/bronze/orders"
silver_path = "data/silver/orders"

bronze_df = (
    spark.read.format("delta").load(bronze_path)
)

silver_df = (
    bronze_df.dropDuplicates(["order_id"]).filter(col("quantity")>0).filter(col("unit_price")>0)
    .withColumn("order_date",to_date(col("order_date"))).withColumn("total_amount",col("quantity")*col("unit_price"))
)

silver_df.show()
silver_df.printSchema()
(
    silver_df.write.format("delta").mode("overwrite").save(silver_path)
)

spark.stop()

