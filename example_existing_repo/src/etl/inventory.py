# inventory report - stock per sku across all the warehouses
# originally a cron script from ops, moved to airflow in 2023
import logging
import os

from src.etl import common

log = logging.getLogger(__name__)

INVENTORY_CSV = os.path.join(common.BASE_DIR, "data/incoming/inventory.csv")
LOW_STOCK = 20
REORDER_TO = 150  # units, agreed with purchasing


def _load():
    return common.read_csv(INVENTORY_CSV)


def getInventory(**kwargs):
    df = _load()
    log.info("inventory rows: %s, warehouses: %s", len(df), sorted(df["warehouse"].unique()))
    if df.empty:
        raise Exception("no inventory data today")
    return len(df)


def buildReport(low_stock_threshold=LOW_STOCK, **kwargs):
    df = _load()  # file is small, just read it again
    df["qty"] = df["qty"].fillna(0).astype(int)
    stock = df[["sku", "qty"]].groupby("sku").sum().reset_index()
    stock["low_stock"] = stock["qty"] < low_stock_threshold
    stock["reorder_qty"] = (REORDER_TO - stock["qty"]).clip(lower=0).where(stock["low_stock"], 0)
    common.write_output(stock, "inventory_by_sku")
    return len(stock)


def publishReport(**kwargs):
    stock = common.read_output("inventory_by_sku")
    stock = stock.sort_values(["low_stock", "qty"], ascending=[False, True])
    common.write_output(stock, "inventory_report")
    n_low = int(stock["low_stock"].sum())
    if n_low:
        log.warning("%s skus below the low stock threshold", n_low)
    return n_low
