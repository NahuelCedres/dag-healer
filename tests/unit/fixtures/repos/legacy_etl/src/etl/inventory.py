# ruff: noqa
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")


def build_report(warehouses):
    import pandas as pd

    stock = pd.read_csv(os.path.join(DATA_DIR, "incoming", "inventory.csv"))
    return stock.merge(pd.read_csv(warehouses))
