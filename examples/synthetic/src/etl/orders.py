"""Orders enrichment: order system exports -> orders mart for the BI team."""

from src.etl.common import read_csv, read_output, write_output

LARGE_ORDER_TOTAL = 250.0
MART_COLUMNS = [
    "order_id",
    "customer_id",
    "items",
    "total",
    "currency",
    "status",
    "avg_item_price",
    "order_date",
    "is_large_order",
]


def extract_orders(**kwargs):
    df = read_csv("data/incoming/orders.csv")
    write_output(df, "orders_raw")
    return len(df)


def average_item_price(total, items):
    """Average price of one item of the order."""
    return round(float(total) / int(items), 2)


def enrich_orders(**kwargs):
    df = read_output("orders_raw")
    df["avg_item_price"] = [
        average_item_price(total, items) for total, items in zip(df["total"], df["items"], strict=True)
    ]
    df["order_date"] = df["created_at"].str[:10]
    df["is_large_order"] = df["total"] >= LARGE_ORDER_TOTAL
    write_output(df, "orders_enriched")
    return len(df)


def load_orders(target="orders_mart", **kwargs):
    df = read_output("orders_enriched")
    write_output(df[MART_COLUMNS], target)
    return len(df)
