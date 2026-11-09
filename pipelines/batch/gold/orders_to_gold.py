"""CommerceFlow | Silver orders -> Gold fact_orders (full snapshot)."""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

REQUIRED_COLUMNS = {"order_id", "customer_id", "status", "created_at"}


def build_fact_orders(orders):
    """Build a one-row-per-order fact table without inventing sales amounts."""
    missing = REQUIRED_COLUMNS.difference(orders.columns)
    if missing:
        raise ValueError(f"Missing Silver orders columns: {sorted(missing)}")

    fact = (
        orders.select(
            F.col("order_id"),
            F.col("customer_id"),
            F.lower(F.trim(F.col("status"))).alias("order_status"),
            F.col("created_at").cast("timestamp").alias("created_at"),
        )
        .withColumn("order_date", F.to_date("created_at"))
        .withColumn("order_count", F.lit(1))
        .withColumn("created_order_count", F.when(F.col("order_status") == "created", 1).otherwise(0))
        .withColumn("paid_order_count", F.when(F.col("order_status") == "paid", 1).otherwise(0))
        .withColumn("cancelled_order_count", F.when(F.col("order_status") == "cancelled", 1).otherwise(0))
        .withColumn("gold_processed_at", F.current_timestamp())
    )

    return fact.select(
        "order_id",
        "customer_id",
        "order_date",
        "created_at",
        "order_status",
        "order_count",
        "created_order_count",
        "paid_order_count",
        "cancelled_order_count",
        "gold_processed_at",
    )


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME"])

    glue_context = GlueContext(SparkContext.getOrCreate())
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    bucket = args["BUCKET_NAME"]
    silver_path = f"s3://{bucket}/silver/orders/"
    gold_path = f"s3://{bucket}/gold/fact_orders/"

    print(f"Reading Silver orders: {silver_path}")
    orders = glue_context.spark_session.read.parquet(silver_path)
    fact_orders = build_fact_orders(orders)

    print("Gold fact_orders schema:")
    fact_orders.printSchema()

    (
        fact_orders.coalesce(2)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(gold_path)
    )

    print(f"Gold fact_orders written to: {gold_path}")
    job.commit()


if __name__ == "__main__":
    main()
