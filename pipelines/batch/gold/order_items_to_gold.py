
"""CommerceFlow | Order items Silver -> Gold fact_order_items."""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


REQUIRED = {"order_item_id", "order_id", "product_id"}
TECHNICAL = {"ingested_at", "silver_processed_at"}


def build_fact_order_items(df):
    missing = REQUIRED - set(df.columns)

    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")

    if df.limit(1).count() == 0:
        raise ValueError("Empty Silver order_items")

    for column in REQUIRED:
        if df.filter(
            F.col(column).isNull()
        ).limit(1).count():
            raise ValueError(f"NULL values in {column}")

    if df.groupBy("order_item_id").count().filter(
        F.col("count") > 1
    ).limit(1).count():
        raise ValueError("Duplicate order_item_id")

    business_columns = [
        c for c in df.columns if c not in TECHNICAL
    ]

    return df.select(*business_columns).withColumn(
        "gold_processed_at", F.current_timestamp()
    )


def main():
    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "BUCKET_NAME"]
    )

    glue = GlueContext(SparkContext.getOrCreate())
    job = Job(glue)
    job.init(args["JOB_NAME"], args)

    bucket = args["BUCKET_NAME"]
    source = f"s3://{bucket}/silver/order_items/"
    target = f"s3://{bucket}/gold/fact_order_items/"

    df = glue.spark_session.read.parquet(source)
    result = build_fact_order_items(df)
    result.printSchema()

    (
        result.coalesce(2).write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(target)
    )

    print(f"Gold fact_order_items written: {target}")
    job.commit()


if __name__ == "__main__":
    main()
