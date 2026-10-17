# ruff: noqa
from datetime import datetime

REFERENCE = "customers_reference.json"


def parse_signup_date(value):
    return datetime.strptime(value, "%Y-%m-%d")


def clean(path):
    import pandas as pd

    df = pd.read_csv(path)
    df["signup_date"] = df["signup_date"].map(parse_signup_date)
    return df.to_dict("records")
