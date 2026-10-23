"""FX rates: daily rates file -> pricing service."""

import json
import os
import urllib.request

from src.etl.common import read_csv, read_output, write_output

# The pricing service runs as a sidecar next to every Airflow worker.
PRICING_API_URL = os.environ.get("PRICING_API_URL", "http://127.0.0.1:8907/v1/fx-rates")


def load_rates(**kwargs):
    df = read_csv("data/incoming/fx_rates.csv")
    latest = df[df["rate_date"] == df["rate_date"].max()]
    write_output(latest, "fx_rates_latest")
    return len(latest)


def publish_rates(**kwargs):
    df = read_output("fx_rates_latest")
    payload = json.dumps({"rates": df.to_dict(orient="records")}).encode("utf-8")
    request = urllib.request.Request(
        PRICING_API_URL, data=payload, method="PUT", headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return response.status
