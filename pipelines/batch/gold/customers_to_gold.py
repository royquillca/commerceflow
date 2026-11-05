
"""CommerceFlow | Customers Silver -> Gold dim_customer."""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


TECHNICAL = {"ingested_at", "silver_processed_at"}


def build_dim_customer(df):
    if "customer_id" not in df.columns:
        raise ValueError("Missing customer_id")

    if df.limit(1).count() == 0:
        raise ValueError("Empty Silver customers")

    if df.filter(
        F.col("customer_id").isNull()
    ).limit(1).count():
        raise ValueError("NULL customer_id")

    if df.groupBy("customer_id").count().filter(
        F.col("count") > 1
    ).limit(1).count():
        raise ValueError("Duplicate customer_id")

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
    source = f"s3://{bucket}/silver/customers/"
    target = f"s3://{bucket}/gold/dim_customer/"

    df = glue.spark_session.read.parquet(source)
    result = build_dim_customer(df)
    result.printSchema()

    (
        result.coalesce(2).write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(target)
    )

    print(f"Gold dim_customer written: {target}")
    job.commit()


if __name__ == "__main__":
    main()
