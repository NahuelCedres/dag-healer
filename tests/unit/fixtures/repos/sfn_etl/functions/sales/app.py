"""Lambda handler of the sales_daily_etl state machine: one ETL step per invocation.

The state machine sends {"step": "extract" | "transform" | "load", "execution": <execution ARN>,
"args": {...}}; the steps themselves live in the shared etl layer (src/etl).
"""

import json
import logging
import os

from etl import sales

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

STEPS = {
    "extract": sales.extract_sales,
    "transform": sales.transform_sales,
    "load": sales.load_sales,
}


def handler(event, context):
    logger.info("received event %s", json.dumps(event))
    step = event["step"]
    rows = STEPS[step](**event.get("args", {}))
    return {"step": step, "rows": rows}
