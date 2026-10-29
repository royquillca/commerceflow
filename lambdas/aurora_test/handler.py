
import os
import time
import boto3
from datetime import datetime, timezone

from generator import CommerceDataGenerator
from aurora_loader import AuroraDataApiLoader
from schema_loader import SchemaLoader


rds = boto3.client("rds-data")

def lambda_handler(event, context):
    
    action = event.get("action", "test")

    # Data Generator
    if action in ("seed_reference", "seed_orders"):
        generator = CommerceDataGenerator(
            seed=2026,
            customer_count=2000,
            product_count=500,
        )

        loader = AuroraDataApiLoader(
            client=rds,
            cluster_arn=os.environ["CLUSTER_ARN"],
            secret_arn=os.environ["SECRET_ARN"],
            database=os.environ["DATABASE_NAME"],
        )

        if action == "seed_reference":
            # Load customers before orders
            for start in range(0, 2000, 100):
                customers = list(generator.customers())
                loader.insert_customers(
                    customers[start:start + 100]
                )

            # Load products before order items
            products = list(generator.products())

            for start in range(0, len(products), 100):
                loader.insert_products(
                    products[start:start + 100]
                )

            return {
                "statusCode": 200,
                "customers_processed": 2000,
                "products_processed": 500,
            }

        # Load a small batch of historical orders
        start_id = int(event.get("start_id", 1))
        count = int(event.get("count", 100))

        if start_id < 1 or count < 1 or count > 100:
            raise ValueError("Invalid range: count must be 1-100")

        records = list(generator.orders(
            start_id=start_id,
            count=count,
            start_date=datetime(
                2025, 10, 1, tzinfo=timezone.utc
            ),
            end_date=datetime(
                2026, 10, 1, tzinfo=timezone.utc
            ),
        ))

        processed = loader.insert_batch(records)

        return {
            "statusCode": 200,
            "orders_processed": processed,
            "start_id": start_id,
        }

    if action == "verify_schema":
        result = rds.execute_statement(
            resourceArn=os.environ["CLUSTER_ARN"],
            secretArn=os.environ["SECRET_ARN"],
            database=os.environ["DATABASE_NAME"],
            sql="""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """
        )

        return {
            "statusCode": 200,
            "tables": [
                row[0]["stringValue"]
                for row in result["records"]
            ]
        }

    if action == "create_schema":
        loader = SchemaLoader(
            rds_client=rds,
            cluster_arn=os.environ["CLUSTER_ARN"],
            secret_arn=os.environ["SECRET_ARN"],
            database=os.environ["DATABASE_NAME"]
        )

        count = loader.execute_file("/var/task/schema.sql")

        return {
            "statusCode": 200,
            "message": "Schema created successfully",
            "statements_executed": count
        }

    if action != "test":
        raise ValueError(f"Unsupported action: {action}")

    for attempt in range(5):
        try:
            result = rds.execute_statement(
                resourceArn=os.environ["CLUSTER_ARN"],
                secretArn=os.environ["SECRET_ARN"],
                database=os.environ["DATABASE_NAME"],
                sql="SELECT 1 AS connection_test"
            )

            return {
                "statusCode": 200,
                "result": result["records"][0][0]["longValue"]
            }

        except rds.exceptions.DatabaseResumingException:
            if attempt == 4:
                raise
            time.sleep(3 * (attempt + 1))
