"""Lambda handler of the inventory_report state machine: one report step per invocation.

The state machine sends {"step": "get_inventory" | "build_report" | "publish_report",
"execution": <execution ARN>, "args": {...}} (build_report runs once per warehouse, inside the
PerWarehouse Map, and also gets "warehouse"); the steps themselves live in the shared etl layer
(src/etl).
"""

import json
import logging
import os

from etl import inventory

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

STEPS = {
    "get_inventory": inventory.getInventory,
    "build_report": inventory.buildReport,
    "publish_report": inventory.publishReport,
}


def handler(event, context):
    logger.info("received event %s", json.dumps(event))
    step = event["step"]
    rows = STEPS[step](**event.get("args", {}))
    return {"step": step, "rows": rows}
