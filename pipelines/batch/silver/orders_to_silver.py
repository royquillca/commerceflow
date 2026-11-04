import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import SelectFromCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


REQUIRED_COLUMNS = {
    "order_id",
    "customer_id",
    "status",
    "created_at",
    "updated_at",
    "ingested_at",
}


DQ_RULESET = """
Rules = [
    IsComplete "order_id",
    IsComplete "customer_id",
    IsComplete "status",
    IsComplete "created_at",
    IsComplete "updated_at",

    ColumnValues "order_id" > 0,
    ColumnValues "customer_id" > 0,

    ColumnValues "status" in ["paid", "pending", "cancelled"],

    IsUnique "order_id"
]
"""

# Evaluating Missing Required Columns
def validate_schema(df):
    missing_columns = REQUIRED_COLUMNS - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}"
        )


def evaluate_quality(glue_context, dynamic_frame):
    return EvaluateDataQuality().process_rows(
        frame=dynamic_frame,
        ruleset=DQ_RULESET,
        publishing_options={
            "dataQualityEvaluationContext": "commerceflow_orders_silver",
            "enableDataQualityCloudWatchMetrics": True,
            "enableDataQualityResultsPublishing": True,
        },
        additional_options={
            "performanceTuning.caching": "CACHE_NOTHING",
            "publishAggregatedMetrics.status": "ENABLED",
        },
    )


def write_parquet(df, path, partitions=1):
    (
        df.coalesce(partitions)
        .write
        .mode("overwrite")
        .parquet(path)
    )


def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "BUCKET_NAME",
        ],
    )

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    bucket = args["BUCKET_NAME"]

    bronze_path = f"s3://{bucket}/bronze/orders/snapshot/"
    silver_path = f"s3://{bucket}/silver/orders/"
    quarantine_path = f"s3://{bucket}/quarantine/orders/"

    # -------------------------
    # 1. Read Bronze
    # -------------------------
    bronze_df = glue_context.spark_session.read.parquet(bronze_path)

    validate_schema(bronze_df)

    # -------------------------
    # 2. Cross-column rule
    # -------------------------
    bronze_df = bronze_df.withColumn(
        "_valid_timestamps",
        F.col("created_at") <= F.col("updated_at"),
    )

    # Convert Spark DataFrame -> Glue DynamicFrame
    bronze_dyf = DynamicFrame.fromDF(
        bronze_df,
        glue_context,
        "bronze_orders",
    )

    # -------------------------
    # 3. AWS Glue Data Quality
    # -------------------------
    dq_collection = evaluate_quality(
        glue_context,
        bronze_dyf,
    )

    row_outcomes = SelectFromCollection.apply(
        dfc=dq_collection,
        key="rowLevelOutcomes",
        transformation_ctx="rowLevelOutcomes",
    )

    dq_df = row_outcomes.toDF()

    # -------------------------
    # 4. Split valid / invalid
    # -------------------------
    valid_df = dq_df.filter(
        (F.col("DataQualityEvaluationResult") == "Passed")
        & (F.col("_valid_timestamps") == True)
    )

    invalid_df = dq_df.filter(
        (F.col("DataQualityEvaluationResult") == "Failed")
        | (F.col("_valid_timestamps") == False)
    )

    # -------------------------
    # 5. Clean Silver schema
    # -------------------------
    quality_columns = [
        "DataQualityRulesPass",
        "DataQualityRulesFail",
        "DataQualityRulesSkip",
        "DataQualityEvaluationResult",
        "_valid_timestamps",
    ]

    silver_df = valid_df.drop(*quality_columns)

    quarantine_df = invalid_df.drop(
        "DataQualityRulesPass",
        "DataQualityRulesSkip",
    )

    # -------------------------
    # 6. Write outputs
    # -------------------------
    write_parquet(
        silver_df,
        silver_path,
        partitions=1,
    )

    write_parquet(
        quarantine_df,
        quarantine_path,
        partitions=1,
    )

    print(f"Bronze rows: {bronze_df.count()}")
    print(f"Silver rows: {silver_df.count()}")
    print(f"Quarantine rows: {quarantine_df.count()}")

    job.commit()


if __name__ == "__main__":
    main()