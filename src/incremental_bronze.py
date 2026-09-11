from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import input_file_name, current_timestamp, lit
from delta.tables import DeltaTable


# --------------------------------------------------
# Spark Session
# --------------------------------------------------

builder = (
    SparkSession.builder
    .appName("EcommerceIncrementalBronze")
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
# Paths
# --------------------------------------------------

raw_path = "data/raw/*.csv"
processed_files_path = "data/bronze/processed_files"
bronze_path = "data/bronze/orders"


# --------------------------------------------------
# Step 1: Find all files
# --------------------------------------------------

all_files_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(raw_path)
    .withColumn("source_file", input_file_name())
)


# --------------------------------------------------
# Step 2: Read processed file tracking table
# --------------------------------------------------

processed_files_df = (
    spark.read
    .format("delta")
    .load(processed_files_path)
)

print("Already processed files:")

processed_files_df.show(truncate=False)


# --------------------------------------------------
# Step 3: Identify NEW / RETRY files
# --------------------------------------------------

successful_files_df = (
    processed_files_df
    .filter(processed_files_df.status == "SUCCESS")
    .select("source_file")
    .distinct()
)


new_files_df = (
    all_files_df
    .select("source_file")
    .distinct()
    .join(
        successful_files_df,
        on="source_file",
        how="left_anti"
    )
)

print("NEW / RETRY FILES:")

new_files_df.show(truncate=False)


# --------------------------------------------------
# Step 4: Get paths of new files
# --------------------------------------------------

new_file_paths = [
    row["source_file"]
    for row in new_files_df.collect()
]


# --------------------------------------------------
# Step 5: Process only if new files exist
# --------------------------------------------------

if not new_file_paths:

    print("No new files to process.")

else:

    print("Files to process:")

    for file_path in new_file_paths:
        print(file_path)


    # --------------------------------------------------
    # Steps 6-9: Process new files
    # --------------------------------------------------

    try:

        # --------------------------------------------------
        # Step 6: Read only NEW files
        # --------------------------------------------------

        incoming_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(new_file_paths)
            .withColumn("source_file", input_file_name())
            .withColumn("ingestion_timestamp", current_timestamp())
        )

        print("New records to process:")

        incoming_df.show()


        # --------------------------------------------------
        # Step 7: Get Bronze Delta table
        # --------------------------------------------------

        bronze_table = DeltaTable.forPath(
            spark,
            bronze_path
        )


        # --------------------------------------------------
        # Step 8: MERGE new records into Bronze
        # --------------------------------------------------

        (
            bronze_table
            .alias("target")
            .merge(
                incoming_df.alias("source"),
                "target.order_id = source.order_id"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        print("Bronze MERGE completed successfully.")


        # --------------------------------------------------
        # Step 9: Record SUCCESS
        # --------------------------------------------------

        processed_records_df = (
            new_files_df
            .withColumn("processed_at", current_timestamp())
            .withColumn("status", lit("SUCCESS"))
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
            .save(processed_files_path)
        )

        print("Processed files tracking updated successfully.")


        # --------------------------------------------------
        # Step 10: Show Bronze result
        # --------------------------------------------------

        print("Bronze record count:")

        print(
            bronze_table.toDF().count()
        )

        print("Bronze data:")

        bronze_table.toDF().orderBy("order_id").show()


    # --------------------------------------------------
    # Step 11: Handle FAILURE
    # --------------------------------------------------

    except Exception as e:

        print("Bronze processing FAILED.")

        error_message = str(e)

        print("Error:")
        print(error_message)


        # --------------------------------------------------
        # Record FAILED files
        # --------------------------------------------------

        failed_records_df = (
            new_files_df
            .withColumn("processed_at", current_timestamp())
            .withColumn("status", lit("FAILED"))
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
            .save(processed_files_path)
        )

        print("Failed files tracking updated successfully.")


# --------------------------------------------------
# Stop Spark
# --------------------------------------------------

spark.stop()