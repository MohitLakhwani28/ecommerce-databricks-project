
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, when


builder = (
    SparkSession.builder
    .appName("EcommerceDataQuality")
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


bronze_path = "data/bronze/orders"


bronze_df = (
    spark.read.format("delta").load(bronze_path)
)

print("Bronze data:")
bronze_df.show(truncate=False)


dq_df = (
    bronze_df.withColumn(
        "dq_reason",
        when(col("order_id").isNull(),"order_id is NULL").
        when(col("customer_id").isNull(),"customer_id is NULL").
        when(col("product_id").isNull(),"product_id is NULL").
        when(col("quantity")<=0,"quantity <=0").                      # represents one PySpark Column object.
        when(col("unit_price")<=0,"unit_price <=0").
        when(~col("status").isin("COMPLETED","CANCELLED"),"invalid status")
    )
)
valid_df = (
    dq_df.filter(col("dq_reason").isNull())
)

invalid_df = (
    dq_df.filter(col("dq_reason").isNotNull())
)

print("Valid records:")
valid_df.show(truncate=False)

print("Invalid records:")
invalid_df.show(truncate=False)

print("Valid record count:")
print(valid_df.count())

print("Invalid record count:")
print(invalid_df.count())



quarantine_path = "data/quarantine/orders"

if invalid_df.count() > 0:

    (
        invalid_df
        .write
        .format("delta")
        .mode("append")
        .save(quarantine_path)
    )

    print("Invalid records appended to quarantine.")

else:

    print("No invalid records to quarantine.")