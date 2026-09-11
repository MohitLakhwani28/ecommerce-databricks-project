from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col,
    count,
    sum,
    current_timestamp,
    lit
)


# --------------------------------------------------
# 1. Configure Spark
# --------------------------------------------------

builder = (
    SparkSession.builder
    .appName("EcommerceIncrementalGold")
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

silver_path = "data/silver/orders"
tracking_path = "data/gold/processed_silver"
gold_path = "data/gold/customer_order_summary"


# --------------------------------------------------
# 3. Read Silver
# --------------------------------------------------

silver_df = (
    spark.read
    .format("delta")
    .load(silver_path)
)


# --------------------------------------------------
# 4. Read Gold Tracking
# --------------------------------------------------

processed_silver_df = (
    spark.read
    .format("delta")
    .load(tracking_path)
)

print("Gold tracking:")

processed_silver_df.show(truncate=False)


# --------------------------------------------------
# 5. Find Successfully Processed Files
# --------------------------------------------------

successful_files_df = (
    processed_silver_df
    .filter(col("status") == "SUCCESS")
    .select("source_file")
    .distinct()
)


# --------------------------------------------------
# 6. Find NEW / RETRY Silver Files
# --------------------------------------------------

silver_files_df = (
    silver_df
    .select("source_file")
    .distinct()
)

new_files_df = (
    silver_files_df
    .join(
        successful_files_df,
        on="source_file",
        how="left_anti"
    )
)

print("NEW / RETRY Silver files:")

new_files_df.show(truncate=False)


# --------------------------------------------------
# 7. Collect New File Paths
# --------------------------------------------------

new_file_paths = [
    row["source_file"]
    for row in new_files_df.collect()
]


# --------------------------------------------------
# 8. Stop if Nothing New
# --------------------------------------------------

if not new_file_paths:

    print("No new Silver files to process.")

else:

    print("Silver files to process:")

    for file_path in new_file_paths:
        print(file_path)

    try:

        # --------------------------------------------------
        # 9. Get New Silver Records
        # --------------------------------------------------

        new_silver_df = (
            silver_df
            .filter(
                col("source_file").isin(new_file_paths)
            )
        )

        print("New Silver records:")

        new_silver_df.show(truncate=False)


        # --------------------------------------------------
        # 10. Find Affected Customers
        # --------------------------------------------------

        affected_customers_df = (
            new_silver_df
            .select("customer_id")
            .distinct()
        )

        print("Affected customers:")

        affected_customers_df.show(truncate=False)


        # --------------------------------------------------
        # 11. Get All Silver Records
        #     For Affected Customers
        # --------------------------------------------------

        affected_silver_df = (
            silver_df
            .join(
                affected_customers_df,
                on="customer_id",
                how="inner"
            )
        )


        # --------------------------------------------------
        # 12. Recalculate Gold Aggregates
        # --------------------------------------------------

        gold_updates_df = (
            affected_silver_df
            .groupBy("customer_id")
            .agg(
                count("order_id").alias("total_orders"),
                sum("quantity").alias("total_quantity"),
                sum("total_amount").alias("total_spending")
            )
        )

        print("Gold updates:")

        gold_updates_df.show(truncate=False)


        # --------------------------------------------------
        # 13. Check/Create Gold Delta Table
        # --------------------------------------------------

        if not DeltaTable.isDeltaTable(
            spark,
            gold_path
        ):

            (
                gold_updates_df
                .write
                .format("delta")
                .mode("overwrite")
                .save(gold_path)
            )

            gold_table = DeltaTable.forPath(
                spark,
                gold_path
            )

            print(
                "Gold Delta table created successfully."
            )

        else:

            gold_table = DeltaTable.forPath(
                spark,
                gold_path
            )


            # --------------------------------------------------
            # 14. MERGE into Gold
            # --------------------------------------------------

            (
                gold_table
                .alias("target")
                .merge(
                    gold_updates_df.alias("source"),
                    "target.customer_id = source.customer_id"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

            print(
                "Gold MERGE completed successfully."
            )


        # --------------------------------------------------
        # 15. Update Gold Tracking - SUCCESS
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

        print(
            "Gold tracking updated successfully."
        )


        # --------------------------------------------------
        # 16. Verify Gold
        # --------------------------------------------------

        print("Gold record count:")

        print(
            gold_table
            .toDF()
            .count()
        )

        print("Gold data:")

        (
            gold_table
            .toDF()
            .orderBy("customer_id")
            .show(truncate=False)
        )


    # --------------------------------------------------
    # 17. Failure Handling
    # --------------------------------------------------

    except Exception as e:

        error_message = str(e)

        print(
            f"Gold processing failed: {error_message}"
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
            "Failed Silver files recorded in Gold tracking."
        )


# --------------------------------------------------
# 18. Stop Spark
# --------------------------------------------------

spark.stop()