
"""CommerceFlow | Products Silver -> Gold dim_product."""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


REQUIRED_COLUMNS = {"product_id"}

TECHNICAL_COLUMNS = {
    "ingested_at",
    "silver_processed_at",
}


def validate_products(df):
    """Validate product dimension keys before publishing Gold."""

    missing = REQUIRED_COLUMNS - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing Silver columns: {sorted(missing)}"
        )

    if df.limit(1).count() == 0:
        raise ValueError("Silver products dataset is empty")

    if df.filter(F.col("product_id").isNull()).limit(1).count() > 0:
        raise ValueError("Silver products contains NULL product_id")

    duplicates = (
        df.groupBy("product_id")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicates > 0:
        raise ValueError("Silver products contains duplicate product_id")


def build_dim_product(products):
    """Build a Type 1 product dimension from validated Silver data."""

    validate_products(products)

    business_columns = [
        column
        for column in products.columns
        if column not in TECHNICAL_COLUMNS
    ]

    dimension = (
        products
        .select(*business_columns)
        .withColumn(
            "gold_processed_at",
            F.current_timestamp(),
        )
    )

    return dimension


def main():
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "BUCKET_NAME"],
    )

    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    bucket = args["BUCKET_NAME"]

    silver_path = f"s3://{bucket}/silver/products/"
    gold_path = f"s3://{bucket}/gold/dim_product/"

    print(f"Reading Silver products: {silver_path}")

    products = spark.read.parquet(silver_path)

    dim_product = build_dim_product(products)

    print("Gold dim_product schema:")
    dim_product.printSchema()

    print(f"Writing Gold products: {gold_path}")

    (
        dim_product.coalesce(2)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(gold_path)
    )

    print(f"Gold dim_product written to: {gold_path}")

    job.commit()


if __name__ == "__main__":
    main()
