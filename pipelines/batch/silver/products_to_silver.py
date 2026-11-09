
"""CommerceFlow | Products Bronze -> Silver."""

import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsgluedq.transforms import EvaluateDataQuality

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


REQUIRED_COLUMNS = {"product_id"}

DQ_RULESET = """
Rules = [
    RowCount > 0,
    IsComplete "product_id",
    IsUnique "product_id"
]
"""


def validate_schema(df):
    """Validate the minimum expected Bronze schema."""

    missing = REQUIRED_COLUMNS - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing required columns: {sorted(missing)}"
        )


def transform_products(df):
    """Keep valid products and quarantine invalid identifiers."""

    validate_schema(df)

    # Count occurrences of each product ID.
    product_window = Window.partitionBy("product_id")

    prepared = df.withColumn(
        "_product_id_count",
        F.count(F.lit(1)).over(product_window),
    )

    invalid_condition = (
        F.col("product_id").isNull()
        | (F.col("_product_id_count") > 1)
    )

    invalid = (
        prepared.filter(invalid_condition)
        .withColumn(
            "quarantine_reason",
            F.when(
                F.col("product_id").isNull(),
                F.lit("NULL_PRODUCT_ID"),
            ).otherwise(
                F.lit("DUPLICATE_PRODUCT_ID")
            ),
        )
        .drop("_product_id_count")
    )

    valid = (
        prepared.filter(~invalid_condition)
        .drop("_product_id_count")
        .withColumn(
            "silver_processed_at",
            F.current_timestamp(),
        )
    )

    return valid, invalid


def validate_data_quality(glue_context, df):
    """Run AWS Glue Data Quality against valid products."""

    dynamic_frame = DynamicFrame.fromDF(
        df,
        glue_context,
        "products_silver",
    )

    results = EvaluateDataQuality.apply(
        frame=dynamic_frame,
        ruleset=DQ_RULESET,
        publishing_options={
            "dataQualityEvaluationContext": "products_silver",
            "enableDataQualityCloudWatchMetrics": True,
            "enableDataQualityResultsPublishing": True,
        },
    )

    results_df = results.toDF()
    results_df.show(truncate=False)

    failed_rules = results_df.filter(
        F.col("Outcome") == "Failed"
    ).count()

    if failed_rules > 0:
        raise ValueError(
            f"Products Data Quality failed: {failed_rules} rules"
        )


def main():
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "BUCKET_NAME"],
    )

    glue_context = GlueContext(
        SparkContext.getOrCreate()
    )

    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    bucket = args["BUCKET_NAME"]

    bronze_path = (
        f"s3://{bucket}/bronze/products/snapshot/"
    )

    silver_path = (
        f"s3://{bucket}/silver/products/"
    )

    quarantine_path = (
        f"s3://{bucket}/quarantine/products/"
    )

    print(f"Reading Bronze products: {bronze_path}")

    products = spark.read.parquet(bronze_path)

    valid, invalid = transform_products(products)

    valid_count = valid.count()
    invalid_count = invalid.count()

    print(f"Valid products: {valid_count}")
    print(f"Invalid products: {invalid_count}")

    if valid_count == 0:
        raise ValueError(
            "No valid products found. Silver will not be overwritten."
        )

    validate_data_quality(
        glue_context,
        valid,
    )

    if invalid_count > 0:
        print(f"Writing quarantine: {quarantine_path}")

        (
            invalid.coalesce(2)
            .write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(quarantine_path)
        )

    print(f"Writing Silver products: {silver_path}")

    (
        valid.coalesce(2)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(silver_path)
    )

    print("Products Bronze -> Silver completed.")

    job.commit()


if __name__ == "__main__":
    main()
