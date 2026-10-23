"""Payments settlement: payment provider exports -> settlement ledger."""

from src.etl.common import read_csv, read_output, write_output

# Processing fee per payment method, as a share of the amount.
FEE_RATES = {"card": 0.029, "bank_transfer": 0.008, "wallet": 0.021}
DEFAULT_FEE_RATE = 0.03


def extract_payments(**kwargs):
    df = read_csv("data/incoming/payments.csv")
    df = df.drop_duplicates("payment_id", keep="last")
    write_output(df, "payments_raw")
    return len(df)


def settle_payments(**kwargs):
    df = read_output("payments_raw")
    df["amount"] = df["amount"].astype(float)
    rates = df["method"].map(FEE_RATES).fillna(DEFAULT_FEE_RATE)
    df["fee"] = (df["amount"] * rates).round(2)
    df["net_amount"] = (df["amount"] - df["fee"]).round(2)
    df["settlement_date"] = df["created_at"].str[:10]
    write_output(df, "payments_settled")
    return len(df)


def load_ledger(target="settlement_ledger", **kwargs):
    df = read_output("payments_settled")
    keys = ["settlement_date", "currency", "psp"]
    ledger = df.groupby(keys, as_index=False)[["amount", "fee", "net_amount"]].sum()
    write_output(ledger.round(2), target)
    return len(ledger)
