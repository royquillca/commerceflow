
"""CommerceFlow | Order items Bronze -> Silver."""

import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


REQUIRED = {"order_item_id", "order_id", "product_id"}

RULESET = """
Rules = [
    RowCount > 0,
    IsComplete "order_item_id",
    IsUnique "order_item_id",
    IsComplete "order_id",
    IsComplete "product_id"
]
"""


def transform_order_items(df):
    missing = REQUIRED - set(df.columns)

    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")

    w = Window.partitionBy("order_item_id")
    checked = df.withColumn(
        "_key_count", F.count("*").over(w)
    )

    invalid_condition = (
        F.col("order_item_id").isNull()
        | F.col("order_id").isNull()
        | F.col("product_id").isNull()
        | (F.col("_key_count") > 1)
    )

    invalid = checked.filter(invalid_condition).withColumn(
        "quarantine_reason",
        F.when(
            F.col("order_item_id").isNull(),
            "NULL_ORDER_ITEM_ID"
        ).when(
            F.col("order_id").isNull(),
            "NULL_ORDER_ID"
        ).when(
            F.col("product_id").isNull(),
            "NULL_PRODUCT_ID"
        ).otherwise("DUPLICATE_ORDER_ITEM_ID")
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
    bronze = f"s3://{bucket}/bronze/order_items/snapshot/"
    silver = f"s3://{bucket}/silver/order_items/"
    quarantine = f"s3://{bucket}/quarantine/order_items/"

    source = glue.spark_session.read.parquet(bronze)
    valid, invalid = transform_order_items(source)

    valid_count = valid.count()
    invalid_count = invalid.count()

    print(f"Valid order items: {valid_count}")
    print(f"Invalid order items: {invalid_count}")

    if valid_count == 0:
        raise ValueError("No valid order items")

    dq = EvaluateDataQuality.apply(
        frame=DynamicFrame.fromDF(
            valid, glue, "order_items_silver"
        ),
        ruleset=RULESET,
        publishing_options={
            "dataQualityEvaluationContext": "order_items_silver",
            "enableDataQualityCloudWatchMetrics": True,
            "enableDataQualityResultsPublishing": True,
        },
    )

    if dq.toDF().filter(
        F.col("Outcome") == "Failed"
    ).count() > 0:
        raise ValueError("Order items Data Quality failed")

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

    print(f"Silver order items written: {silver}")
    job.commit()


if __name__ == "__main__":
    main()
