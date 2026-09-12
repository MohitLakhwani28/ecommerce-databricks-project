from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, to_date, current_timestamp, lit
from delta.tables import DeltaTable
from src.data_quality import apply_data_quality

# --------------------------------------------------
# 1. Configure Spark
# --------------------------------------------------

builder = (
    SparkSession.builder
    .appName("EcommerceIncrementalSilver")
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
# 2. Paths
# --------------------------------------------------

bronze_path = "data/bronze/orders"
tracking_path = "data/silver/processed_bronze"
silver_path = "data/silver/orders"


# --------------------------------------------------
# 3. Read Bronze
# --------------------------------------------------

bronze_df = (
    spark.read
    .format("delta")
    .load(bronze_path)
)


# --------------------------------------------------
# 4. Read Silver Tracking Table
# --------------------------------------------------

processed_bronze_df = (
    spark.read
    .format("delta")
    .load(tracking_path)
)

print("Silver tracking:")

processed_bronze_df.show(truncate=False)


# --------------------------------------------------
# 5. Find Successfully Processed Bronze Files
# --------------------------------------------------

successful_files_df = (
    processed_bronze_df
    .filter(col("status") == "SUCCESS")
    .select("source_file")
    .distinct()
)


# --------------------------------------------------
# 6. Find NEW / RETRY Bronze Files
# --------------------------------------------------

bronze_files_df = (
    bronze_df
    .select("source_file")
    .distinct()
)

new_files_df = (
    bronze_files_df
    .join(
        successful_files_df,
        on="source_file",
        how="left_anti"
    )
)

print("NEW / RETRY Bronze files:")

new_files_df.show(truncate=False)


# --------------------------------------------------
# 7. Collect New File Names
# --------------------------------------------------

new_file_paths = [
    row["source_file"]
    for row in new_files_df.collect()
]


# --------------------------------------------------
# 8. Stop if Nothing New
# --------------------------------------------------

if not new_file_paths:

    print("No new Bronze files to process.")

else:

    print("Bronze files to process:")

    for file_path in new_file_paths:
        print(file_path)


    try:

        # --------------------------------------------------
        # 9. Select Only New Bronze Records
        # --------------------------------------------------

        incoming_df = (
            bronze_df
            .filter(
                col("source_file").isin(new_file_paths)
            )
        )

        print("New Bronze records:")

        incoming_df.show(truncate=False)


        # --------------------------------------------------
        # 10. Silver Transformations
        # --------------------------------------------------
        
        dq_df = apply_data_quality(incoming_df)
        valid_df = (
            dq_df.filter(col("dq_reason").isNull())
        )
        invalid_df = (
            dq_df.filter(col("dq_reason").isNotNull())
        )
        quarantine_path = "data/quarantine/orders"
        if  invalid_df.count() > 0:
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
        print("Invalid records:")
        invalid_df.show(truncate=False)

        print("Valid records:")
        valid_df.show(truncate=False)

        silver_df = (
           valid_df
           .dropDuplicates(["order_id"])
           .drop("dq_reason")
           .withColumn("order_date",to_date(col("order_date")))
           .withColumn("total_amount",col("quantity")*col("unit_price"))
        )

        print("Silver records:")

        silver_df.show(truncate=False)

        silver_df.printSchema()


        # --------------------------------------------------
        # 11. Check/Create Silver Delta Table
        # --------------------------------------------------

        if not DeltaTable.isDeltaTable(spark, silver_path):

            (
                silver_df
                .write
                .format("delta")
                .mode("overwrite")
                .save(silver_path)
            )

            silver_table = DeltaTable.forPath(
                spark,
                silver_path
            )

            print("Silver Delta table created successfully.")


        else:

            silver_table = DeltaTable.forPath(
                spark,
                silver_path
            )

            # --------------------------------------------------
            # 12. MERGE into Silver
            # --------------------------------------------------

            (
                silver_table
                .alias("target")
                .merge(
                    silver_df.alias("source"),
                    "target.order_id = source.order_id"
                )
                .whenNotMatchedInsertAll()
                .execute()
            )

            print("Silver MERGE completed successfully.")


        # --------------------------------------------------
        # 13. Update Silver Tracking
        # --------------------------------------------------

        processed_records_df = (
            new_files_df
            .withColumn(
                "processed_at",
                current_timestamp()
            )
            .withColumn(
                "status",
                lit("SUCCESS")
            )
            .withColumn(
                "error_message",
                lit(None).cast("string")
            )
        )

        (
            processed_records_df
            .write
            .format("delta")
            .mode("append")
            .save(tracking_path)
        )

        print("Silver tracking updated successfully.")


        # --------------------------------------------------
        # 14. Verify Silver
        # --------------------------------------------------

        print("Silver Record count:")

        print(
            silver_table
            .toDF()
            .count()
        )


        print("Silver data:")

        (
            silver_table
            .toDF()
            .orderBy("order_id")
            .show(truncate=False)
        )


    # --------------------------------------------------
    # 15. Failure Handling
    # --------------------------------------------------

    except Exception as e:

        error_message = str(e)

        print(
            f"Silver processing failed: {error_message}"
        )


        failed_records_df = (
            new_files_df
            .withColumn(
                "processed_at",
                current_timestamp()
            )
            .withColumn(
                "status",
                lit("FAILED")
            )
            .withColumn(
                "error_message",
                lit(error_message)
            )
        )


        (
            failed_records_df
            .write
            .format("delta")
            .mode("append")
            .save(tracking_path)
        )

        print(
            "Failed Bronze files recorded in Silver tracking."
        )


# --------------------------------------------------
# 16. Stop Spark
# --------------------------------------------------

spark.stop()