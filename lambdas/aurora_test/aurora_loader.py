
from datetime import timezone
from decimal import Decimal


def sql_parameter(name, value):
    """Convert a Python value to an RDS Data API parameter."""

    if value is None:
        encoded = {"isNull": True}

    elif isinstance(value, Decimal):
        return {
            "name": name,
            "value": {"stringValue": str(value)},
            "typeHint": "DECIMAL",
        }

    elif hasattr(value, "tzinfo"):
        if value.tzinfo is None:
            raise ValueError("Datetime must include a timezone")

        utc_value = value.astimezone(timezone.utc)

        encoded = {
            "stringValue": utc_value.strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )
        }

        return {
            "name": name,
            "value": encoded,
            "typeHint": "TIMESTAMP",
        }

    elif isinstance(value, int):
        encoded = {"longValue": value}

    else:
        encoded = {"stringValue": str(value)}

    return {"name": name, "value": encoded}


class AuroraDataApiLoader:
    """Insert records into Aurora using RDS Data API."""

    def __init__(self, client, cluster_arn, secret_arn, database):
        self.client = client
        self.cluster_arn = cluster_arn
        self.secret_arn = secret_arn
        self.database = database

    def _request(self):
        """Return common arguments for Data API calls."""
        return {
            "resourceArn": self.cluster_arn,
            "secretArn": self.secret_arn,
            "database": self.database,
        }

    def _transaction_request(self):
        """Arguments for committing or rolling back transactions."""
        return {
            "resourceArn": self.cluster_arn,
            "secretArn": self.secret_arn,
        }

    def _insert(self, table, columns, rows, transaction_id):
        """Insert multiple records using one SQL statement template."""

        if not rows:
            return

        placeholders = ", ".join(f":{col}" for col in columns)

        sql = f"""
            INSERT INTO {table} ({", ".join(columns)})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING
        """

        parameter_sets = [
            [
                sql_parameter(column, row[column])
                for column in columns
            ]
            for row in rows
        ]

        self.client.batch_execute_statement(
            **self._request(),
            sql=sql,
            parameterSets=parameter_sets,
            transactionId=transaction_id,
        )

    def insert_batch(self, records):
        """Insert all records atomically."""

        if not records:
            return 0

        transaction = self.client.begin_transaction(
            **self._request()
        )

        transaction_id = transaction["transactionId"]

        try:
            self._insert(
                "orders",
                ["order_id", "customer_id", "status",
                 "created_at", "updated_at"],
                [r["order"] for r in records],
                transaction_id,
            )

            self._insert(
                "order_items",
                ["order_item_id", "order_id", "product_id",
                 "quantity", "unit_price"],
                [r["item"] for r in records],
                transaction_id,
            )

            self._insert(
                "payments",
                ["payment_id", "order_id", "amount",
                 "status", "paid_at", "created_at"],
                [r["payment"] for r in records],
                transaction_id,
            )

            self.client.commit_transaction(
                **self._transaction_request(),
                transactionId=transaction_id,
            )

        except Exception:
            self.client.rollback_transaction(
                **self._transaction_request(),
                transactionId=transaction_id,
            )
            raise

        return len(records)

    def insert_customers(self, customers):
        return self._insert_reference_data(
            "customers",
            ["customer_id", "full_name", "email", "region"],
            customers,
        )

    def insert_products(self, products):
        return self._insert_reference_data(
            "products",
            ["product_id", "product_name", "category", "price"],
            products,
        )

    def _insert_reference_data(self, table, columns, rows):
        rows = list(rows)

        if not rows:
            return 0

        transaction = self.client.begin_transaction(
            **self._request()
        )
        transaction_id = transaction["transactionId"]

        try:
            self._insert(table, columns, rows, transaction_id)

            self.client.commit_transaction(
                **self._transaction_request(),
                transactionId=transaction_id,
            )
        except Exception:
            self.client.rollback_transaction(
                **self._transaction_request(),
                transactionId=transaction_id,
            )
            raise

        return len(rows)
