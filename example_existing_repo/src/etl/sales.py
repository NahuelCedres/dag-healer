"""Sales daily ETL: POS export -> daily revenue summary per currency."""
import pandas as pd

from src.etl import common
from src.etl.common import read_csv, write_output

SALES_FILE = "data/incoming/sales.csv"

# TODO: read the rates from the finance FX table instead of hardcoding them (DATA-412)
FX_TO_USD = {"USD": 1.0, "EUR": 1.08, "GBP": 1.27}


def extract_sales(**context):
    df = read_csv(SALES_FILE)
    print("extracted %d rows from %s" % (len(df), SALES_FILE))
    if len(df) == 0:
        raise ValueError("sales file is empty!!")
    # the POS sometimes re-sends the same order
    df = df.drop_duplicates(subset=["order_id"], keep="last")
    write_output(df, "sales_staging")
    return len(df)


def transform_sales(report_currency="USD", **context):
    df = common.read_output("sales_staging")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df[df["amount"] > 0].copy()  # refunds are handled by finance, not here

    if report_currency != "USD":
        raise NotImplementedError("only USD reports for now")
    df["amount_usd"] = df["amount"] * df["currency"].map(FX_TO_USD)

    summary = (
        df.groupby(["order_date", "currency"])
        .agg(
            orders=("order_id", "count"),
            revenue=("amount", "sum"),
            revenue_usd=("amount_usd", "sum"),
        )
        .reset_index()
    )
    summary["revenue"] = summary["revenue"].round(2)
    summary["revenue_usd"] = summary["revenue_usd"].round(2)
    write_output(summary, "sales_daily_summary")
    print("total revenue: %.2f %s" % (summary["revenue_usd"].sum(), report_currency))
    return len(summary)


def load_sales(**context):
    # the analytics.sales_daily table was dropped in the warehouse migration,
    # finance reads the csv directly now
    summary = common.read_output("sales_daily_summary")
    summary["source"] = "pos"
    write_output(summary, "sales_daily_report")
    return len(summary)


def send_report(**context):
    # disabled in the DAG, finance moved to the new SMTP relay
    raise NotImplementedError("send_report is disabled")
