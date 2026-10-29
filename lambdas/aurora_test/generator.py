
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal


class CommerceDataGenerator:
    """Generate reproducible synthetic e-commerce data."""

    def __init__(
        self,
        seed: int = 2026,
        customer_count: int = 2000,
        product_count: int = 500,
    ):
        if customer_count < 1 or product_count < 1:
            raise ValueError("Counts must be positive")

        self.seed = seed
        self.customer_count = customer_count
        self.product_count = product_count

    def customers(self):
        regions = ["Lima", "Cusco", "Arequipa", "Piura"]

        for customer_id in range(1, self.customer_count + 1):
            rng = random.Random(self.seed + customer_id)

            yield {
                "customer_id": customer_id,
                "full_name": f"Customer {customer_id}",
                "email": f"customer{customer_id}@example.com",
                "region": rng.choice(regions),
            }

    def products(self):
        categories = ["Electronics", "Clothing", "Home", "Sports"]

        for product_id in range(1, self.product_count + 1):
            rng = random.Random(self.seed + product_id)

            yield {
                "product_id": product_id,
                "product_name": f"Product {product_id}",
                "category": rng.choice(categories),
                "price": Decimal(rng.randint(1000, 100000)) / 100,
            }

    def orders(
        self,
        start_id: int,
        count: int,
        start_date: datetime,
        end_date: datetime,
    ):
        if start_id < 1 or count < 0:
            raise ValueError("Invalid order range")

        if start_date.tzinfo is None or end_date.tzinfo is None:
            raise ValueError("Dates must include timezone information")

        if end_date <= start_date:
            raise ValueError("end_date must be after start_date")

        period_seconds = int((end_date - start_date).total_seconds())

        for order_id in range(start_id, start_id + count):
            rng = random.Random(self.seed + order_id)

            customer_id = rng.randint(1, self.customer_count)
            product_id = rng.randint(1, self.product_count)

            quantity = rng.randint(1, 5)
            unit_price = Decimal(rng.randint(1000, 100000)) / 100
            amount = unit_price * quantity

            created_at = start_date + timedelta(
                seconds=rng.randrange(period_seconds)
            )

            status = rng.choices(
                ["paid", "pending", "cancelled"],
                weights=[80, 15, 5],
                k=1,
            )[0]

            payment_status = {
                "paid": "completed",
                "pending": "pending",
                "cancelled": "failed",
            }[status]

            yield {
                "order": {
                    "order_id": order_id,
                    "customer_id": customer_id,
                    "status": status,
                    "created_at": created_at,
                    "updated_at": created_at,
                },
                "item": {
                    "order_item_id": order_id,
                    "order_id": order_id,
                    "product_id": product_id,
                    "quantity": quantity,
                    "unit_price": unit_price,
                },
                "payment": {
                    "payment_id": order_id,
                    "order_id": order_id,
                    "amount": amount,
                    "status": payment_status,
                    "paid_at": created_at if status == "paid" else None,
                    "created_at": created_at,
                },
            }
