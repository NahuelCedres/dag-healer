"""Lambda handler of the customers-sync state machine: one ETL step per invocation.

The state machine sends {"step": "extract" | "transform" | "load", "execution": <execution ARN>,
"requested_by": <who started the sync>, "args": {...}}; the steps themselves live in the shared
etl layer (src/etl).
"""

import json
import logging
import os

from etl import customers

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

STEPS = {
    "extract": customers.extract_customers,
    "transform": customers.transform_customers,
    "load": customers.load_customers,
}


def handler(event, context):
    logger.info("received event %s", json.dumps(event))
    step = event["step"]
    rows = STEPS[step](**event.get("args", {}))
    return {"step": step, "rows": rows}
