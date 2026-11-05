
"""CommerceFlow | Payments Bronze -> Silver."""

import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


REQUIRED = {"payment_id", "order_id"}

RULESET = """
Rules = [
    RowCount > 0,
    IsComplete "payment_id",
    IsUnique "payment_id",
    IsComplete "order_id"
]
"""


def transform_payments(df):
    missing = REQUIRED - set(df.columns)

    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")

    w = Window.partitionBy("payment_id")
    checked = df.withColumn(
        "_key_count", F.count("*").over(w)
    )

    invalid_condition = (
        F.col("payment_id").isNull()
        | F.col("order_id").isNull()
        | (F.col("_key_count") > 1)
    )

    invalid = checked.filter(invalid_condition).withColumn(
        "quarantine_reason",
        F.when(
            F.col("payment_id").isNull(),
            "NULL_PAYMENT_ID"
        ).when(
            F.col("order_id").isNull(),
            "NULL_ORDER_ID"
        ).otherwise("DUPLICATE_PAYMENT_ID")
    ).drop("_key_count")

    valid = checked.filter(
        ~invalid_condition
    ).drop("_key_count").withColumn(
        "silver_processed_at", F.current_timestamp()
    )

    return valid, invalid


def main():
    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "BUCKET_NAME"]
    )

    glue = GlueContext(SparkContext.getOrCreate())
    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    bucket = args["BUCKET_NAME"]
    bronze = f"s3://{bucket}/bronze/payments/snapshot/"
    silver = f"s3://{bucket}/silver/payments/"
    quarantine = f"s3://{bucket}/quarantine/payments/"

    source = glue.spark_session.read.parquet(bronze)
    valid, invalid = transform_payments(source)

    valid_count = valid.count()
    invalid_count = invalid.count()

    print(f"Valid payments: {valid_count}")
    print(f"Invalid payments: {invalid_count}")

    if valid_count == 0:
        raise ValueError("No valid payments")

    dq = EvaluateDataQuality.apply(
        frame=DynamicFrame.fromDF(
            valid, glue, "payments_silver"
        ),
        ruleset=RULESET,
        publishing_options={
            "dataQualityEvaluationContext": "payments_silver",
            "enableDataQualityCloudWatchMetrics": True,
            "enableDataQualityResultsPublishing": True,
        },
    )

    if dq.toDF().filter(
        F.col("Outcome") == "Failed"
    ).count() > 0:
        raise ValueError("Payments Data Quality failed")

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

    print(f"Silver payments written: {silver}")
    job.commit()


if __name__ == "__main__":
    main()
