import sys
import time

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


SUPPORTED_TABLES = {
    "orders",
    "customers",
    "products",
    "payments",
    "order_items",
}

MAX_CONNECTION_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 20


def validate_source_table(table):
    """Allow only the OLTP tables expected by CommerceFlow."""

    if table not in SUPPORTED_TABLES:
        raise ValueError(
            f"Unsupported source table: {table}. "
            f"Allowed tables: {sorted(SUPPORTED_TABLES)}"
        )


def read_aurora(glue_context, connection_name, table):
    """Read a PostgreSQL table through the existing Glue JDBC connection."""

    for attempt in range(1, MAX_CONNECTION_ATTEMPTS + 1):
        try:
            print(
                f"Reading public.{table} from Aurora "
                f"(attempt {attempt}/{MAX_CONNECTION_ATTEMPTS})"
            )

            dynamic_frame = glue_context.create_dynamic_frame.from_options(
                connection_type="postgresql",
                connection_options={
                    "useConnectionProperties": "true",
                    "connectionName": connection_name,
                    "dbtable": f"public.{table}",
                },
                transformation_ctx=f"read_{table}",
            )

            return dynamic_frame.toDF()

        except Exception:
            if attempt == MAX_CONNECTION_ATTEMPTS:
                raise

            print(
                "Aurora connection failed. "
                f"Retrying in {RETRY_DELAY_SECONDS} seconds..."
            )

            time.sleep(RETRY_DELAY_SECONDS)


def add_ingestion_metadata(df):
    """Add metadata describing when Glue ingested the record."""

    return df.withColumn(
        "ingested_at",
        F.current_timestamp(),
    )


def write_bronze(df, bucket, table):
    """Write a full Bronze snapshot as Snappy Parquet."""

    output_path = f"s3://{bucket}/bronze/{table}/snapshot/"

    (
        df.coalesce(2)
        .write
        .mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .save(output_path)
    )

    return output_path


def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "BUCKET_NAME",
            "CONNECTION_NAME",
            "SOURCE_TABLE",
        ],
    )

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    table = args["SOURCE_TABLE"]

    validate_source_table(table)

    df = read_aurora(
        glue_context=glue_context,
        connection_name=args["CONNECTION_NAME"],
        table=table,
    )

    df = add_ingestion_metadata(df)

    print(f"Source table: public.{table}")
    print("Source schema:")
    df.printSchema()

    output_path = write_bronze(
        df=df,
        bucket=args["BUCKET_NAME"],
        table=table,
    )

    print(f"Bronze snapshot written to: {output_path}")

    job.commit()


if __name__ == "__main__":
    main()