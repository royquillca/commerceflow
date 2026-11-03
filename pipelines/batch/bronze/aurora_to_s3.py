
import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


def read_aurora(glue_context, connection_name, table):
    """Read a PostgreSQL table through a Glue JDBC connection."""

    dynamic_frame = glue_context.create_dynamic_frame.from_options(
        connection_type="postgresql",
        connection_options={
            "useConnectionProperties": "true",
            "connectionName": connection_name,
            "dbtable": f"public.{table}",
        },
    )

    return dynamic_frame.toDF()


def add_ingestion_metadata(df):
    """Add metadata to the extracted DataFrame."""

    return df.withColumn(
        "ingested_at",
        F.current_timestamp(),
    )


def write_bronze(df, bucket, table):
    """Write the first full snapshot as Parquet."""

    output_path = f"s3://{bucket}/bronze/{table}/snapshot/"

    (
        df.write
        .mode("overwrite")
        .format("parquet")
        .save(output_path)
    )

    return output_path


def main():
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "BUCKET_NAME", "CONNECTION_NAME", "SOURCE_TABLE"],
    )

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)

    table = args["SOURCE_TABLE"]

    if table not in {"orders", "customers", "products", "payments", "order_items"}:
        raise ValueError(f"Unsupported source table: {table}")

    df = read_aurora(
        glue_context,
        args["CONNECTION_NAME"],
        table,
    )

    df = add_ingestion_metadata(df)

    path = write_bronze(
        df,
        args["BUCKET_NAME"],
        table,
    )

    print(f"Bronze snapshot written to: {path}")


if __name__ == "__main__":
    main()
