
"""CommerceFlow | Customers Bronze -> Silver."""

import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


RULESET = """
Rules = [
    RowCount > 0,
    IsComplete "customer_id",
    IsUnique "customer_id"
]
"""


def transform_customers(df):
    if "customer_id" not in df.columns:
        raise ValueError("Missing customer_id")

    w = Window.partitionBy("customer_id")

    checked = df.withColumn(
        "_key_count", F.count("*").over(w)
    )

    invalid = checked.filter(
        F.col("customer_id").isNull()
        | (F.col("_key_count") > 1)
    ).withColumn(
        "quarantine_reason",
        F.when(
            F.col("customer_id").isNull(),
            "NULL_CUSTOMER_ID"
        ).otherwise("DUPLICATE_CUSTOMER_ID")
    ).drop("_key_count")

    valid = checked.filter(
        F.col("customer_id").isNotNull()
        & (F.col("_key_count") == 1)
    ).drop("_key_count").withColumn(
        "silver_processed_at", F.current_timestamp()
    )

    return valid, invalid


def main():
    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "BUCKET_NAME"]
    )
    glue = GlueContext(SparkContext.getOrCreate())
    spark = glue.spark_session

    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    bucket = args["BUCKET_NAME"]
    bronze = f"s3://{bucket}/bronze/customers/snapshot/"
    silver = f"s3://{bucket}/silver/customers/"
    quarantine = f"s3://{bucket}/quarantine/customers/"

    source = spark.read.parquet(bronze)
    valid, invalid = transform_customers(source)

    valid_count = valid.count()
    invalid_count = invalid.count()

    print(f"Valid customers: {valid_count}")
    print(f"Invalid customers: {invalid_count}")

    if valid_count == 0:
        raise ValueError("No valid customers")

    dq = EvaluateDataQuality.apply(
        frame=DynamicFrame.fromDF(
            valid, glue, "customers_silver"
        ),
        ruleset=RULESET,
        publishing_options={
            "dataQualityEvaluationContext": "customers_silver",
            "enableDataQualityCloudWatchMetrics": True,
            "enableDataQualityResultsPublishing": True,
        },
    )

    if dq.toDF().filter(
        F.col("Outcome") == "Failed"
    ).count() > 0:
        raise ValueError("Customers Data Quality failed")

    if invalid_count > 0:
        (
            invalid.coalesce(2).write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(quarantine)
        )

    (
        valid.coalesce(2).write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(silver)
    )

    print(f"Silver customers written: {silver}")
    job.commit()


if __name__ == "__main__":
    main()
