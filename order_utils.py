import random
from typing import Optional

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]


def generate_order(seq: int, invalid_rate: float = 0.1) -> dict:
    order_id = str(1000 + seq)
    product = random.choice(PRODUCTS)
    price = round(random.uniform(5.0, 500.0), 2)
    if random.random() < invalid_rate:
        price = -abs(price)
    return {"orderId": order_id, "product": product, "price": price}


def validate_order(order: dict) -> Optional[str]:
    if not order.get("orderId"):
        return "missing_order_id"
    if not order.get("product"):
        return "missing_product"
    if order.get("price", 0) < 0:
        return "negative_price"
    return None
