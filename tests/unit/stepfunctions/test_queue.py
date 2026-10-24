"""Tests of :mod:`dag_healer.stepfunctions.queue`: SQS receive and acknowledgement batching.

Calls are answered by ``botocore.stub.Stubber`` with the exact request parameters.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import pytest

from dag_healer.core.errors import NotFound, OrchestratorError
from dag_healer.core.ports import HintQueue, QueuedMessage
from dag_healer.stepfunctions.intake import SfnIntake
from dag_healer.stepfunctions.queue import MAX_MESSAGES, WAIT_TIME_SECONDS, SfnQueue

from .conftest import load_json, sfn_options

QUEUE_URL = "https://sqs.eu-west-1.amazonaws.com/123456789012/healer-sfn-events"


@pytest.fixture
def sqs(stubbed: Callable[[str], tuple[Any, Any]]) -> Any:
    return stubbed("sqs")[1]


@pytest.fixture
def queue(make_runtime: Callable[..., Any]) -> SfnQueue:
    return SfnQueue(make_runtime(sfn_options(queue_url=QUEUE_URL)), QUEUE_URL)


def receive_params(count: int) -> dict[str, Any]:
    return {"QueueUrl": QUEUE_URL, "MaxNumberOfMessages": count, "WaitTimeSeconds": WAIT_TIME_SECONDS}


def delete_params(receipts: list[str]) -> dict[str, Any]:
    entries = [{"Id": str(position), "ReceiptHandle": receipt} for position, receipt in enumerate(receipts)]
    return {"QueueUrl": QUEUE_URL, "Entries": entries}


def deleted(receipts: list[str]) -> dict[str, Any]:
    return {"Successful": [{"Id": str(position)} for position in range(len(receipts))], "Failed": []}


def test_the_queue_is_a_hint_queue(queue: SfnQueue) -> None:
    assert isinstance(queue, HintQueue)
    assert (MAX_MESSAGES, WAIT_TIME_SECONDS) == (10, 1)


# =========================================================================== receive


def test_receive_keeps_the_message_id_and_body_only(sqs: Any, queue: SfnQueue) -> None:
    answer = load_json("api/detection/receive_message.json")
    sqs.add_response("receive_message", answer, receive_params(10))
    messages = queue.receive()
    assert [message.receipt for message in messages] == [item["ReceiptHandle"] for item in answer["Messages"]]
    for message, item in zip(messages, answer["Messages"], strict=True):
        assert isinstance(message, QueuedMessage)
        assert message.channel == "eventbridge"
        assert message.payload == {"MessageId": item["MessageId"], "Body": item["Body"]}


@pytest.mark.parametrize(("max_items", "count"), [(1, 1), (3, 3), (10, 10), (25, 10)])
def test_receive_asks_for_at_most_ten(sqs: Any, queue: SfnQueue, max_items: int, count: int) -> None:
    sqs.add_response("receive_message", {}, receive_params(count))
    assert queue.receive(max_items=max_items) == []


@pytest.mark.parametrize("max_items", [0, -3])
def test_receive_nothing_without_a_call(queue: SfnQueue, max_items: int) -> None:
    assert queue.receive(max_items=max_items) == []


def test_messages_without_receipt_handle_are_skipped(sqs: Any, queue: SfnQueue) -> None:
    sqs.add_response(
        "receive_message",
        {"Messages": [{"MessageId": "m-1", "Body": "{}"}, {"MessageId": "m-2", "ReceiptHandle": "r-2"}]},
        receive_params(10),
    )
    (message,) = queue.receive()
    assert message == QueuedMessage(
        receipt="r-2", channel="eventbridge", payload={"MessageId": "m-2", "Body": ""}
    )


def test_receive_errors_are_core_errors(sqs: Any, queue: SfnQueue) -> None:
    sqs.add_client_error(
        "receive_message",
        service_error_code="AWS.SimpleQueueService.NonExistentQueue",
        http_status_code=400,
        expected_params=receive_params(10),
    )
    with pytest.raises(NotFound):
        queue.receive()
    sqs.add_client_error(
        "receive_message",
        service_error_code="ServiceUnavailable",
        http_status_code=503,
        expected_params=receive_params(10),
    )
    with pytest.raises(OrchestratorError) as raised:
        queue.receive()
    assert raised.value.transient is True


def test_received_messages_normalise_to_sqs_hints(sqs: Any, queue: SfnQueue) -> None:
    sqs.add_response("receive_message", load_json("api/detection/receive_message.json"), receive_params(10))
    intake = SfnIntake(queue.rt)
    hints = [
        hint for message in queue.receive() for hint in intake.normalize(message.channel, message.payload, {})
    ]
    assert [(hint.ref.pipeline_id, hint.ref.run_id, hint.source) for hint in hints] == [
        ("customers_sync", "customers-2026-10-02", "sqs"),
        ("inventory_report", "inventory-2026-10-01", "sqs"),
    ]


# =========================================================================== ack


def test_ack_deletes_in_batches_of_ten(sqs: Any, queue: SfnQueue) -> None:
    receipts = [f"receipt-{number:02d}" for number in range(23)]
    for start in (0, 10, 20):
        batch = receipts[start : start + 10]
        sqs.add_response("delete_message_batch", deleted(batch), delete_params(batch))
    queue.ack(receipts)


def test_ack_skips_duplicates_and_empty_receipts(sqs: Any, queue: SfnQueue) -> None:
    sqs.add_response("delete_message_batch", deleted(["a", "b"]), delete_params(["a", "b"]))
    queue.ack(["a", "", "b", "a"])


def test_ack_of_nothing_makes_no_call(queue: SfnQueue) -> None:
    queue.ack([])
    queue.ack(["", ""])


def test_partial_delete_failures_are_logged_not_raised(
    sqs: Any, queue: SfnQueue, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING)
    answer = {
        "Successful": [{"Id": "0"}],
        "Failed": [
            {"Id": "1", "SenderFault": True, "Code": "ReceiptHandleIsInvalid", "Message": "expired r-2"}
        ],
    }
    sqs.add_response("delete_message_batch", answer, delete_params(["r-1", "r-2"]))
    queue.ack(["r-1", "r-2"])
    (record,) = [record for record in caplog.records if record.name == "dag_healer.stepfunctions.queue"]
    assert "1 of 2" in record.getMessage() and "ReceiptHandleIsInvalid" in record.getMessage()
    assert "r-2" not in record.getMessage()


def test_ack_errors_are_core_errors(sqs: Any, queue: SfnQueue) -> None:
    sqs.add_client_error(
        "delete_message_batch",
        service_error_code="AccessDenied",
        http_status_code=403,
        expected_params=delete_params(["r-1"]),
    )
    with pytest.raises(OrchestratorError) as raised:
        queue.ack(["r-1"])
    assert raised.value.transient is False
