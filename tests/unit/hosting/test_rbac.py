"""RBAC of the standalone host (``hosting/rbac.py``, section 7.3 of ``docs/architecture/orchestrators.md``).

``RuleAuthorizer`` answers the questions the API asks Airflow (``check_dag_access``,
``authorized_dag_ids``, ``can_read_dag``, ``can_edit_dag``) from ``serve.auth.rules``.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

import pytest
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.testclient import TestClient
from pydantic import ValidationError

from dag_healer.api import auth
from dag_healer.api.auth import Authorizer, Principal
from dag_healer.config import RbacRule, ServeAuthConfig, config_from_mapping
from dag_healer.hosting.rbac import (
    ACT,
    APPROVE_CONTRACT,
    DEV_RULES,
    READ,
    RuleAuthorizer,
    action_for,
    effective_rules,
)

ALICE = Principal(id="alice", name="Alice", groups=frozenset({"data-eng"}))
BOB = Principal(id="bob", name="Bob", groups=frozenset({"analysts", "oncall"}))
NOBODY = Principal(id="carol", name="Carol")
READ_TASK_INSTANCES = auth.require_dag_access("GET", "TASK_INSTANCE")


@dataclass
class AirflowLikeUser:
    """A user object that is not a Principal (for example an Airflow user)."""

    name: str

    def get_id(self) -> str:
        return self.name

    def get_name(self) -> str:
        return self.name


def _request(**path_params: str) -> Request:
    return Request({"type": "http", "method": "GET", "path": "/", "headers": [], "path_params": path_params})


def _check(authorizer: RuleAuthorizer, user: Any, method: str, entity: str | None, **path: str) -> None:
    asyncio.run(authorizer.check_dag_access(_request(**path), user, method, entity))


def _rule(
    principals: list[str], pipelines: list[str] | None = None, actions: list[str] | None = None
) -> RbacRule:
    data: dict[str, Any] = {"principals": principals}
    if pipelines is not None:
        data["pipelines"] = pipelines
    if actions is not None:
        data["actions"] = actions
    return RbacRule.model_validate(data)


# --------------------------------------------------------------------------- actions


@pytest.mark.parametrize(
    ("method", "entity", "action"),
    [
        ("GET", "TASK_INSTANCE", READ),
        ("GET", "CODE", READ),
        ("GET", "RUN", READ),
        ("GET", None, READ),
        ("get", "TASK_INSTANCE", READ),
        ("PUT", "TASK_INSTANCE", ACT),
        ("PUT", "CODE", APPROVE_CONTRACT),
        ("PUT", None, APPROVE_CONTRACT),
        ("PUT", "RUN", None),
        ("PUT", "TASK_LOGS", None),
        ("POST", "TASK_INSTANCE", None),
        ("POST", None, None),
        ("DELETE", "TASK_INSTANCE", None),
        ("PATCH", None, None),
    ],
)
def test_action_for(method: str, entity: str | None, action: str | None) -> None:
    assert action_for(method, entity) == action


def test_the_rule_authorizer_is_an_authorizer() -> None:
    assert isinstance(RuleAuthorizer([]), Authorizer)


# --------------------------------------------------------------------------- principals


@pytest.mark.parametrize(
    ("principal", "user", "allowed"),
    [
        ("user:alice", ALICE, True),
        ("user:alice", BOB, False),
        ("user:Alice", ALICE, False),  # ids are exact and case-sensitive (the name does not count)
        ("group:data-eng", ALICE, True),
        ("group:data-eng", BOB, False),
        ("group:oncall", BOB, True),
        ("authenticated", NOBODY, True),
        ("*", NOBODY, True),
        ("user:alice", AirflowLikeUser("alice"), False),
        ("group:data-eng", AirflowLikeUser("alice"), False),
        ("authenticated", AirflowLikeUser("alice"), True),
        ("*", AirflowLikeUser("alice"), True),
        ("authenticated", None, False),
        ("*", None, False),
    ],
)
def test_principals(principal: str, user: Any, allowed: bool) -> None:
    authorizer = RuleAuthorizer([_rule([principal], ["*"], [READ])])
    assert authorizer.allows(user, "sales_daily_etl", READ) is allowed


def test_any_principal_of_a_rule_may_match() -> None:
    authorizer = RuleAuthorizer([_rule(["user:bob", "group:data-eng"], ["*"], [READ])])
    assert authorizer.allows(ALICE, "x", READ)
    assert authorizer.allows(BOB, "x", READ)
    assert not authorizer.allows(NOBODY, "x", READ)


# --------------------------------------------------------------------------- pipelines


@pytest.mark.parametrize(
    ("patterns", "pipeline", "allowed"),
    [
        (["*"], "sales_daily_etl", True),
        (["sales_*"], "sales_daily_etl", True),
        (["sales_*"], "customers_sync", False),
        (["Sales_*"], "sales_daily_etl", False),  # case-sensitive, like monitor.dags
        (["sales_daily_etl"], "sales_daily_etl", True),
        (["sales_?aily_etl", "x"], "sales_daily_etl", True),
        (["*", "!secret_*"], "secret_dag", False),  # an exclusion wins
        (["*", "!secret_*"], "sales_daily_etl", True),
        (["secret_*", "!secret_*"], "secret_dag", False),
        (["!secret_*"], "sales_daily_etl", True),  # only exclusions: every other pipeline
        (["!secret_*"], "secret_dag", False),
        ([], "sales_daily_etl", False),  # an empty list matches nothing
    ],
)
def test_pipeline_patterns(patterns: list[str], pipeline: str, allowed: bool) -> None:
    authorizer = RuleAuthorizer([_rule(["*"], patterns, [READ])])
    assert authorizer.allows(ALICE, pipeline, READ) is allowed


def test_an_empty_pipeline_id_is_never_allowed() -> None:
    authorizer = RuleAuthorizer([_rule(["*"], ["*"], [READ, ACT, APPROVE_CONTRACT])])
    assert not authorizer.allows(ALICE, "", READ)
    assert not authorizer.allows(ALICE, None, READ)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- actions and rules


def test_actions_are_independent() -> None:
    authorizer = RuleAuthorizer([_rule(["user:alice"], ["*"], [ACT])])
    assert authorizer.allows(ALICE, "sales_daily_etl", ACT)
    assert not authorizer.allows(ALICE, "sales_daily_etl", READ)  # act does not imply read
    assert not authorizer.allows(ALICE, "sales_daily_etl", APPROVE_CONTRACT)
    assert not authorizer.allows(ALICE, "sales_daily_etl", "admin")  # unknown action


def test_rule_defaults_read_every_pipeline() -> None:
    authorizer = RuleAuthorizer([RbacRule(principals=["user:alice"])])
    assert authorizer.allows(ALICE, "anything", READ)
    assert not authorizer.allows(ALICE, "anything", ACT)


def test_any_allowing_rule_wins() -> None:
    authorizer = RuleAuthorizer(
        [
            _rule(["authenticated"], ["sales_*"], [READ]),
            _rule(["group:data-eng"], ["*", "!secret_*"], [READ, ACT]),
            _rule(["user:bob"], ["customers_sync"], [APPROVE_CONTRACT]),
        ]
    )
    assert authorizer.allows(NOBODY, "sales_daily_etl", READ)
    assert not authorizer.allows(NOBODY, "customers_sync", READ)
    assert authorizer.allows(ALICE, "customers_sync", ACT)
    assert not authorizer.allows(ALICE, "secret_dag", READ)  # excluded by its only matching rule
    assert authorizer.allows(BOB, "customers_sync", APPROVE_CONTRACT)
    assert not authorizer.allows(BOB, "customers_sync", ACT)


def test_without_rules_everything_is_denied() -> None:
    authorizer = RuleAuthorizer([], candidates=lambda: {"sales_daily_etl"})
    assert not authorizer.allows(ALICE, "sales_daily_etl", READ)
    assert not authorizer.can_read_dag(ALICE, "sales_daily_etl")
    assert not authorizer.can_edit_dag(ALICE, "sales_daily_etl")
    assert authorizer.authorized_dag_ids(ALICE) == set()
    with pytest.raises(HTTPException) as denied:
        _check(authorizer, ALICE, "GET", "TASK_INSTANCE", dag_id="sales_daily_etl")
    assert denied.value.status_code == 403


def test_mapping_rules_are_validated() -> None:
    rule = {"principals": "group:data-eng", "pipelines": ["sales_*"], "actions": ["act"]}
    authorizer = RuleAuthorizer([rule])
    assert authorizer.rules == (
        RbacRule(principals=["group:data-eng"], pipelines=["sales_*"], actions=[ACT]),
    )
    assert authorizer.allows(ALICE, "sales_daily_etl", ACT)
    with pytest.raises(ValidationError):
        RuleAuthorizer([{"principals": ["role:admin"]}])
    with pytest.raises(ValidationError):
        RuleAuthorizer([{"principals": ["*"], "actions": ["delete"]}])
    with pytest.raises(TypeError):
        RuleAuthorizer(["group:data-eng"])  # type: ignore[list-item]


def test_rules_from_healer_yaml() -> None:
    cfg = config_from_mapping(
        {
            "serve": {
                "auth": {
                    "backend": "token",
                    "rules": [
                        {"principals": ["group:data-eng"], "pipelines": ["*"], "actions": ["read", "act"]},
                        {"principals": ["authenticated"], "pipelines": ["sales_*"]},
                    ],
                }
            }
        }
    )
    authorizer = RuleAuthorizer(effective_rules(cfg.serve.auth))
    assert authorizer.can_edit_dag(ALICE, "customers_sync")
    assert authorizer.can_read_dag(BOB, "sales_daily_etl")
    assert not authorizer.can_read_dag(BOB, "customers_sync")


# --------------------------------------------------------------------------- the Authorizer protocol


def test_check_dag_access_reads_the_path_pipeline() -> None:
    authorizer = RuleAuthorizer(
        [
            _rule(["group:data-eng"], ["sales_*"], [READ, ACT]),
            _rule(["user:alice"], ["*"], [APPROVE_CONTRACT]),
        ],
    )
    _check(authorizer, ALICE, "GET", "TASK_INSTANCE", dag_id="sales_daily_etl")
    _check(authorizer, ALICE, "PUT", "TASK_INSTANCE", dag_id="sales_daily_etl")
    _check(authorizer, ALICE, "PUT", None, dag_id="customers_sync")
    _check(authorizer, ALICE, "GET", "CODE", dag_id="sales_weekly")
    for method, entity, dag_id in [
        ("GET", "TASK_INSTANCE", "customers_sync"),  # no rule for this pipeline
        ("PUT", "RUN", "sales_daily_etl"),  # no action
        ("POST", "TASK_INSTANCE", "sales_daily_etl"),
    ]:
        with pytest.raises(HTTPException) as denied:
            _check(authorizer, ALICE, method, entity, dag_id=dag_id)
        assert (denied.value.status_code, denied.value.detail) == (403, "Forbidden")


def test_check_dag_access_without_a_path_pipeline_is_denied() -> None:
    authorizer = RuleAuthorizer([_rule(["*"], ["*"], [READ, ACT, APPROVE_CONTRACT])])
    with pytest.raises(HTTPException) as denied:
        _check(authorizer, ALICE, "GET", "TASK_INSTANCE")
    assert (denied.value.status_code, denied.value.detail) == (403, "Forbidden")


def test_require_dag_access_answers_forbidden_through_the_rules() -> None:
    authorizer = RuleAuthorizer([_rule(["user:alice"], ["sales_*"], [READ])])
    app = FastAPI()

    @app.get("/dags/{dag_id}/incidents")
    def incidents(dag_id: str, user: Any = Depends(READ_TASK_INSTANCES)) -> Any:
        return {"dag_id": dag_id, "user": auth.user_name(user)}

    def current_user(request: Request) -> Principal:
        return ALICE if request.headers.get("x-user") == "alice" else BOB

    app.dependency_overrides[auth.get_current_user] = current_user
    app.dependency_overrides[auth.get_authorizer] = lambda: authorizer
    client = TestClient(app)
    allowed = client.get("/dags/sales_daily_etl/incidents", headers={"X-User": "alice"})
    assert allowed.status_code == 200
    assert allowed.json() == {"dag_id": "sales_daily_etl", "user": "Alice"}
    for dag_id, user in [("customers_sync", "alice"), ("sales_daily_etl", "bob")]:
        denied = client.get(f"/dags/{dag_id}/incidents", headers={"X-User": user})
        assert denied.status_code == 403
        assert denied.json() == {"detail": "Forbidden"}


def test_authorized_dag_ids_filters_the_candidates() -> None:
    calls: list[int] = []

    def candidates() -> list[str]:
        calls.append(1)
        return ["sales_daily_etl", "customers_sync", "secret_dag", ""]

    rules = [
        _rule(["group:data-eng"], ["*", "!secret_*"], [READ]),
        _rule(["user:alice"], ["customers_*"], [ACT]),
    ]
    authorizer = RuleAuthorizer(rules, candidates=candidates)
    assert authorizer.authorized_dag_ids(ALICE) == {"sales_daily_etl", "customers_sync"}
    assert authorizer.authorized_dag_ids(ALICE, "PUT") == {"customers_sync"}
    assert authorizer.authorized_dag_ids(ALICE, "DELETE") == set()
    assert authorizer.authorized_dag_ids(BOB) == set()
    assert len(calls) == 3  # asked on every question (DELETE needs no candidates)


def test_authorized_dag_ids_without_candidates() -> None:
    authorizer = RuleAuthorizer([_rule(["*"], ["*"], [READ])])
    assert authorizer.authorized_dag_ids(ALICE) == set()


def test_authorized_dag_ids_fails_closed_when_the_candidates_cannot_be_listed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def broken() -> list[str]:
        raise OSError("contracts directory unreadable")

    authorizer = RuleAuthorizer([_rule(["*"], ["*"], [READ])], candidates=broken)
    with caplog.at_level(logging.WARNING, logger="dag_healer.hosting.rbac"):
        assert authorizer.authorized_dag_ids(ALICE) == set()
    assert "cannot be listed (OSError)" in caplog.text
    assert authorizer.can_read_dag(ALICE, "sales_daily_etl")  # single checks still answer


def test_can_read_and_can_edit_map_to_the_actions() -> None:
    authorizer = RuleAuthorizer(
        [
            _rule(["user:alice"], ["*"], [READ]),
            _rule(["user:bob"], ["*"], [ACT]),
            _rule(["user:carol"], ["*"], [APPROVE_CONTRACT]),
        ]
    )
    assert authorizer.can_read_dag(ALICE, "d") and not authorizer.can_edit_dag(ALICE, "d")
    assert authorizer.can_edit_dag(BOB, "d") and authorizer.can_edit_dag(BOB, "d", "TASK_INSTANCE")
    assert not authorizer.can_read_dag(BOB, "d") and not authorizer.can_edit_dag(BOB, "d", None)
    assert authorizer.can_edit_dag(NOBODY, "d", None)  # approving the contract: PUT on the pipeline
    assert authorizer.can_edit_dag(NOBODY, "d", "CODE")
    assert not authorizer.can_edit_dag(NOBODY, "d")
    assert not authorizer.can_edit_dag(BOB, "d", "RUN")  # no action: denied


# --------------------------------------------------------------------------- effective rules


def test_the_dev_backend_gets_the_built_in_rule() -> None:
    assert effective_rules(ServeAuthConfig(backend="dev")) == list(DEV_RULES)
    dev = Principal(id="dev", name="dev", groups=frozenset({"admin"}))
    authorizer = RuleAuthorizer(effective_rules(ServeAuthConfig(backend="dev")))
    assert all(authorizer.allows(dev, "any_pipeline", action) for action in (READ, ACT, APPROVE_CONTRACT))
    assert not authorizer.allows(NOBODY, "any_pipeline", READ)


def test_configured_rules_replace_the_built_in_rule() -> None:
    rules = [RbacRule(principals=["user:dev"], pipelines=["sales_*"])]
    assert effective_rules(ServeAuthConfig(backend="dev", rules=rules)) == rules


def test_other_backends_without_rules_deny_everything(caplog: pytest.LogCaptureFixture) -> None:
    sha = "0" * 64
    auth_cfg = ServeAuthConfig(backend="token", tokens=[{"name": "ci", "sha256": sha}])
    with caplog.at_level(logging.WARNING, logger="dag_healer.hosting.rbac"):
        assert effective_rules(auth_cfg) == []
    assert "serve.auth.rules is empty: every request of the token backend is denied" in caplog.text
