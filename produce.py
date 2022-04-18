import random
import requests

from datetime import datetime
from dataclasses import dataclass, asdict
from typing import List
from time import sleep
from uuid import uuid4


@dataclass
class Purchase:
    purchase_id: str
    stock_code: int
    item_description: str
    quantity: int
    customer_id: int
    cost: float
    purchase_date: str


@dataclass
class Product:
    stock_code: int
    item_description: str
    price: float


products = [
    Product(stock_code=1, item_description="Toothbrush", price=9.99),
    Product(stock_code=2, item_description="Pencil", price=0.99),
    Product(stock_code=3, item_description="Mug", price=5),
    Product(stock_code=4, item_description="Typewriter", price=99),
    Product(stock_code=5, item_description="Mobile", price=175),
]


def generate() -> List[Purchase]:
    purchases = []
    for _ in range(random.randint(1, 25)):
        product = random.choice(products)
        purchase = Purchase(
            purchase_id=str(uuid4()),
            stock_code=product.stock_code,
            item_description=product.item_description,
            quantity=random.randint(1, 10),
            customer_id=str(uuid4()),
            cost=product.price,
            purchase_date=datetime.now().isoformat(),
        )
        purchases.append(purchase)

    return purchases


def post_purchases(purchases: List[Purchase]):
    for purchase in purchases:
        try:
            requests.post("http://127.0.0.1:80/produce/sales", json=asdict(purchase))
        except requests.exceptions.ConnectionError as e:
            print(e)
            # We don't care...
            return
    print(f"Sent {len(purchases)} purchases to Kafka...")


def main() -> None:

    while True:
        purchases = generate()
        post_purchases(purchases)
        sleep(0.5)
        return


if __name__ == "__main__":
    main()
