# ruff: noqa
import os

BASE = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CLEAN = os.path.join(BASE, "data", "clean", "sales_clean.parquet")


def transform():
    import pandas as pd

    df = pd.read_csv(os.path.join(BASE, "data", "incoming", "sales.csv"))
    df.to_parquet(CLEAN)


def load():
    import pandas as pd

    return pd.read_parquet(CLEAN)
