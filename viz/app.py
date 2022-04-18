from mongoengine import (
    connect,
    Document,
    DateTimeField,
    StringField,
    IntField,
    FloatField,
)
import pandas as pd
from time import sleep
import streamlit as st
import os

from dotenv import load_dotenv
load_dotenv()

MONGO_HOST = os.getenv("MONGO_HOST", "mongodb://127.0.0.1:27017/company_one")


class Sales(Document):
    purchase_id = StringField(required=True)
    stock_code = IntField(required=True)
    item_description = StringField(required=True)
    quantity = IntField(required=True)
    customer_id = StringField(required=True)
    cost = FloatField(required=True)
    purchase_date = DateTimeField(required=True)


def fetch_counts_per_product_id() -> pd.DataFrame:
    pipeline = [{"$sortByCount": "$item_description"}]
    count_by_product = Sales.objects().aggregate(pipeline)
    rows = [_ for _ in count_by_product]
    if not rows:
        raise NoDataException()
    return pd.DataFrame(rows).set_index("_id")


def fetch_total_sales() -> str:
    pipeline = [
        {
            "$group": {
                "_id": None,
                "totalSaleAmount": {"$sum": {"$multiply": ["$cost", "$quantity"]}},
            }
        }
    ]

    total_sales = Sales.objects().aggregate(pipeline)
    rows = [_ for _ in total_sales]
    if not rows:
        raise NoDataException()

    total = rows[0]["totalSaleAmount"]
    return "£{:,.2f}".format(total)


def update_bar(placeholder) -> None:
    try:
        counts = fetch_counts_per_product_id()
    except NoDataException:
        placeholder.text("Awaiting data...")
        return

    placeholder.bar_chart(counts["count"])


def update_sales(placeholder) -> None:
    try:
        total_sales = fetch_total_sales()
    except NoDataException:
        placeholder.text("Awaiting data...")
        return

    placeholder.header(total_sales)


class NoDataException(Exception):
    pass


def main() -> None:
    connect(host=MONGO_HOST)

    st.header("Company X Live Sales!")
    placeholder_bar = st.empty()
    st.header("Total Sales")
    placeholder_sales = st.empty()

    while True:
        update_bar(placeholder_bar)
        update_sales(placeholder_sales)
        sleep(0.5)


if __name__ == "__main__":
    main()
