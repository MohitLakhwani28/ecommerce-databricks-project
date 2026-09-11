from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import count, sum


# 1. Configure Spark
builder = (
    SparkSession.builder
    .appName("EcommerceGoldTransformation")
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


# 3. Define paths
silver_path = "data/silver/orders"
gold_path = "data/gold/customer_order_summary"


silver_df = (
    spark.read.format("delta").load(silver_path)
)

gold_df = (
    silver_df.groupBy("customer_id")
    .agg(count("order_id").alias("total_orders"),
         sum("quantity").alias("total_quantity"),
         sum("total_amount").alias("total_spending"))
)
gold_df.show()
gold_df.printSchema()

(
    gold_df.write.format("delta").mode("overwrite").save(gold_path)
)
spark.stop()