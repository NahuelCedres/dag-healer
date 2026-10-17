"""CRM customers sync: CRM export -> customers dim."""
import re
from datetime import datetime

from src.etl.common import read_csv, read_output, write_output

US = "US"


def extract_customers(**kwargs):
    df = read_csv("data/incoming/customers.csv", dtype=str)
    df = df.drop_duplicates("customer_id", keep="last")
    write_output(df, "customers_raw")
    return len(df)


def parse_signup_date(value):
    """Parse the signup date sent by the CRM export (always YYYY-MM-DD)."""
    return datetime.strptime(value.strip(), "%Y-%m-%d").date()


def normalize_phone(value):
    if not isinstance(value, str) or not value.strip():
        return None
    digits = re.sub(r"\D", "", value)
    if len(digits) == 10:  # US/CA number without the country code
        digits = "1" + digits
    return "+" + digits


def transform_customers(**kwargs):
    df = read_output("customers_raw", dtype=str)
    df["email"] = df["email"].str.strip().str.lower()
    df["phone"] = df["phone"].apply(normalize_phone)
    df["country"] = df["country"].str.strip().str.upper()
    df["signup_date"] = df["signup_date"].apply(parse_signup_date)
    df["signup_month"] = df["signup_date"].apply(lambda d: d.strftime("%Y-%m"))
    df["is_domestic"] = df["country"] == US
    write_output(df, "customers_clean")
    return len(df)


def load_customers(target="customers_dim", **kwargs):
    df = read_output("customers_clean", dtype=str)
    cols = [
        "customer_id",
        "full_name",
        "email",
        "phone",
        "country",
        "signup_date",
        "signup_month",
        "is_domestic",
    ]
    write_output(df[cols], target)
    return len(df)
