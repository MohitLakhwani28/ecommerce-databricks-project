from pyspark.sql.functions import col, when


def apply_data_quality(df):
    return (
        df.withColumn(
            "dq_reason",
            when(col("order_id").isNull(), "order_id is NULL")
            .when(col("customer_id").isNull(), "customer_id is NULL")
            .when(col("product_id").isNull(), "product_id is NULL")
            .when(col("quantity") <= 0, "quantity <= 0")
            .when(col("unit_price") <= 0, "unit_price <= 0")
            .when(
                ~col("status").isin("COMPLETED", "CANCELLED"),
                "invalid status"
            )
        )
    )
