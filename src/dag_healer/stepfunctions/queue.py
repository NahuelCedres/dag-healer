"""The durable hint queue: EventBridge -> SQS (section 9.4 of the design).

``receive()`` is ``ReceiveMessage(QueueUrl, MaxNumberOfMessages=min(max_items, 10),
WaitTimeSeconds=1)``; each message becomes ``QueuedMessage(receipt=<ReceiptHandle>,
channel="eventbridge", payload={"MessageId": ..., "Body": <body text>})`` (only those two
keys: the push intake parses the body and marks the hint's source ``sqs``). ``ack()`` deletes
the messages with ``DeleteMessageBatch`` (10 per call) once the core committed their pending
events. Delivery is at-least-once; the incident key dedupes. AWS errors propagate as
:mod:`dag_healer.core.errors` exceptions.

A message without a receipt handle cannot be acknowledged and is skipped (SQS delivers it again
and, after the queue's ``maxReceiveCount``, moves it to the dead-letter queue). Entries that
``DeleteMessageBatch`` reports as failed (an expired receipt handle ...) are logged by count and
error code only: those messages come back and are deduplicated by the incident key. Message
bodies and receipt handles are never logged.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from typing import Any

from dag_healer.core.ports import QueuedMessage
from dag_healer.stepfunctions.aws import call
from dag_healer.stepfunctions.events import CHANNEL
from dag_healer.stepfunctions.runtime import SfnRuntime

logger = logging.getLogger(__name__)

MAX_MESSAGES = 10
WAIT_TIME_SECONDS = 1


def _message(raw: Any) -> QueuedMessage | None:
    """The queued message of one ``ReceiveMessage`` item (``None`` without a receipt handle)."""
    if not isinstance(raw, Mapping):
        return None
    receipt = raw.get("ReceiptHandle")
    if not isinstance(receipt, str) or not receipt:
        return None
    message_id = raw.get("MessageId")
    body = raw.get("Body")
    return QueuedMessage(
        receipt=receipt,
        channel=CHANNEL,
        payload={
            "MessageId": message_id if isinstance(message_id, str) else None,
            "Body": body if isinstance(body, str) else "",
        },
    )


def _batches(receipts: Sequence[str]) -> list[list[str]]:
    """Unique, non-empty receipt handles (first occurrence order) in batches of 10."""
    unique = list(dict.fromkeys(r for r in receipts if isinstance(r, str) and r))
    return [unique[start : start + MAX_MESSAGES] for start in range(0, len(unique), MAX_MESSAGES)]


class SfnQueue:
    """``HintQueue`` port over the SQS queue ``queue_url``."""

    def __init__(self, rt: SfnRuntime, queue_url: str) -> None:
        self.rt = rt
        self.queue_url = queue_url

    def __repr__(self) -> str:
        return f"SfnQueue(instance={self.rt.instance!r})"

    def receive(self, *, max_items: int = MAX_MESSAGES) -> list[QueuedMessage]:
        """Up to ``min(max_items, 10)`` messages (one long poll of one second; ``[]`` when empty)."""
        count = min(int(max_items), MAX_MESSAGES)
        if count <= 0:
            return []
        response = call(
            self.rt.clients.sqs,
            "receive_message",
            what="ReceiveMessage",
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=count,
            WaitTimeSeconds=WAIT_TIME_SECONDS,
        )
        raw_messages = response.get("Messages")
        items = raw_messages if isinstance(raw_messages, list) else []
        messages = [message for message in (_message(raw) for raw in items) if message is not None]
        if len(messages) != len(items):
            logger.debug(
                "dag-healer stepfunctions %s: skipped %d SQS message(s) without a receipt handle",
                self.rt.instance,
                len(items) - len(messages),
            )
        logger.debug(
            "dag-healer stepfunctions %s: received %d SQS message(s)", self.rt.instance, len(messages)
        )
        return messages

    def ack(self, receipts: Sequence[str]) -> None:
        """Delete the messages of ``receipts`` (``DeleteMessageBatch``, 10 per call)."""
        for batch in _batches(receipts):
            entries = [
                {"Id": str(position), "ReceiptHandle": receipt} for position, receipt in enumerate(batch)
            ]
            response = call(
                self.rt.clients.sqs,
                "delete_message_batch",
                what="DeleteMessageBatch",
                QueueUrl=self.queue_url,
                Entries=entries,
            )
            failed = response.get("Failed")
            if isinstance(failed, list) and failed:
                codes = sorted({str(item.get("Code")) for item in failed if isinstance(item, Mapping)})
                logger.warning(
                    "dag-healer stepfunctions %s: SQS did not delete %d of %d message(s) (%s); "
                    "they will be delivered again",
                    self.rt.instance,
                    len(failed),
                    len(entries),
                    ", ".join(codes) or "no error code",
                )


__all__ = ["MAX_MESSAGES", "WAIT_TIME_SECONDS", "SfnQueue"]
