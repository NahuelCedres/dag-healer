"""Permissions of the standalone host: the ``serve.auth.rules`` of ``healer.yaml``.

:class:`RuleAuthorizer` is the :class:`~dag_healer.api.auth.Authorizer` of ``dag-healer serve``.
The host builds it and hands it to the authentication backend, whose ``authorizer()`` returns
it, so ``require_dag_access`` and the list routes ask it exactly what they ask Airflow: a method
and a ``DagAccessEntity`` name. :func:`action_for` maps those to the three neutral actions of a
rule:

* ``GET`` with any entity -> ``read`` (lists, incidents, task instances, PR.md, contracts);
* ``PUT TASK_INSTANCE`` -> ``act`` (resolve, retry repair, retry task);
* ``PUT CODE`` and ``PUT`` on the pipeline itself (entity ``None``, which the contract approval
  uses) -> ``approve_contract``;
* anything else -> no action, which is denied.

A rule allows an action on a pipeline when one of its ``principals`` matches the user and its
``pipelines`` patterns match the pipeline id:

* principals: ``user:<id>`` (the :class:`~dag_healer.api.auth.Principal` id, exact and
  case-sensitive), ``group:<name>`` (one of the principal's groups), ``authenticated`` and
  ``*`` (any authenticated user). A user object that is not a ``Principal`` matches only the
  last two;
* pipelines: :func:`fnmatch.fnmatchcase` patterns with the semantics of ``monitor.dags``
  (:meth:`dag_healer.config.HealerConfig.is_monitored`): a ``!pattern`` excludes and wins, a
  list of exclusions only means every other pipeline, an empty list matches none.

Actions are independent (``act`` does not imply ``read``). Any allowing rule wins; without one
the request fails closed with 403 ``Forbidden``. The rules are compiled once, so they are static
for the process: ``dag-healer serve`` reads them at start.
"""

from __future__ import annotations

import fnmatch
import logging
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException, Request, status

from dag_healer.api.auth import Principal
from dag_healer.config import RbacRule, ServeAuthConfig

log = logging.getLogger(__name__)

READ = "read"
ACT = "act"
APPROVE_CONTRACT = "approve_contract"
#: Every action a rule can grant (``serve.auth.rules[].actions``).
ACTIONS: tuple[str, ...] = (READ, ACT, APPROVE_CONTRACT)
FORBIDDEN_DETAIL = "Forbidden"
#: The rules of the ``dev`` backend when ``serve.auth.rules`` is empty: its user ``dev`` (group
#: ``admin``) may do everything on every pipeline.
DEV_RULES: tuple[RbacRule, ...] = (
    RbacRule(principals=["group:admin"], pipelines=["*"], actions=[READ, ACT, APPROVE_CONTRACT]),
)
#: Principals that match every authenticated user.
_ANYONE = frozenset({"authenticated", "*"})


def action_for(method: str, entity: str | None) -> str | None:
    """The neutral action of an API permission question (``None``: there is none, denied).

    ``GET`` with any entity is ``read``; ``PUT`` is ``act`` on ``TASK_INSTANCE`` and
    ``approve_contract`` on ``CODE`` or on the pipeline itself (``entity=None``).
    """
    verb = str(method).upper()
    if verb == "GET":
        return READ
    if verb == "PUT":
        if entity == "TASK_INSTANCE":
            return ACT
        if entity is None or entity == "CODE":
            return APPROVE_CONTRACT
    return None


def effective_rules(auth: ServeAuthConfig) -> list[RbacRule]:
    """The rules ``dag-healer serve`` enforces for the ``serve.auth`` section ``auth``.

    ``auth.rules`` when there are any. Without rules the ``dev`` backend gets
    :data:`DEV_RULES`, and every other backend none: every request is then denied, which is
    logged as a warning.
    """
    if auth.rules:
        return list(auth.rules)
    if auth.backend == "dev":
        log.info(
            "dag-healer: serve.auth.rules is empty: the dev user (group admin) may read, act on "
            "and approve the contract of every pipeline"
        )
        return [rule.model_copy(deep=True) for rule in DEV_RULES]  # the constants stay intact
    log.warning(
        "dag-healer: serve.auth.rules is empty: every request of the %s backend is denied "
        "(403 Forbidden); add rules to healer.yaml",
        auth.backend,
    )
    return []


def _as_rule(rule: RbacRule | Mapping[str, Any]) -> RbacRule:
    """``rule`` itself, or the validated :class:`~dag_healer.config.RbacRule` of a mapping."""
    if isinstance(rule, RbacRule):
        return rule
    if isinstance(rule, Mapping):
        return RbacRule.model_validate(dict(rule))
    raise TypeError(f"an RBAC rule must be an RbacRule or a mapping, not {type(rule).__name__}")


def _named(principals: Iterable[str], kind: str) -> frozenset[str]:
    """The names of the ``<kind>:<name>`` principals."""
    prefix = f"{kind}:"
    return frozenset(principal[len(prefix) :] for principal in principals if principal.startswith(prefix))


@dataclass(frozen=True)
class _CompiledRule:
    """One rule, ready for matching."""

    anyone: bool
    users: frozenset[str]
    groups: frozenset[str]
    includes: tuple[str, ...]
    excludes: tuple[str, ...]
    actions: frozenset[str]

    @classmethod
    def of(cls, rule: RbacRule) -> _CompiledRule:
        return cls(
            anyone=any(principal in _ANYONE for principal in rule.principals),
            users=_named(rule.principals, "user"),
            groups=_named(rule.principals, "group"),
            includes=tuple(pattern for pattern in rule.pipelines if not pattern.startswith("!")),
            excludes=tuple(pattern[1:] for pattern in rule.pipelines if pattern.startswith("!")),
            actions=frozenset(rule.actions),
        )

    def matches_user(self, user: Any) -> bool:
        if user is None:
            return False
        if self.anyone:
            return True
        if not isinstance(user, Principal):
            return False
        return user.id in self.users or not self.groups.isdisjoint(user.groups)

    def matches_pipeline(self, pipeline_id: str) -> bool:
        if any(fnmatch.fnmatchcase(pipeline_id, pattern) for pattern in self.excludes):
            return False
        if not self.includes:
            return bool(self.excludes)
        return any(fnmatch.fnmatchcase(pipeline_id, pattern) for pattern in self.includes)

    def allows(self, user: Any, pipeline_id: str, action: str) -> bool:
        return action in self.actions and self.matches_user(user) and self.matches_pipeline(pipeline_id)


class RuleAuthorizer:
    """:class:`~dag_healer.api.auth.Authorizer` answering from RBAC rules (see the module docstring).

    ``rules`` are :class:`~dag_healer.config.RbacRule` objects or mappings of the same shape,
    validated here. ``candidates`` lists the pipeline ids :meth:`authorized_dag_ids` may return;
    it is called on every such question (the standalone host's candidates are the contracts and
    the configured pipelines; the list routes add the pipelines of their incidents themselves).
    """

    def __init__(
        self,
        rules: Iterable[RbacRule | Mapping[str, Any]],
        *,
        candidates: Callable[[], Iterable[str]] = tuple,
    ) -> None:
        self.rules: tuple[RbacRule, ...] = tuple(_as_rule(rule) for rule in rules)
        self._compiled = tuple(_CompiledRule.of(rule) for rule in self.rules)
        self._candidates = candidates

    def allows(self, user: Any, pipeline_id: str, action: str) -> bool:
        """True when a rule lets ``user`` do ``action`` on ``pipeline_id`` (never for an empty id)."""
        if not isinstance(pipeline_id, str) or not pipeline_id:
            return False
        return any(rule.allows(user, pipeline_id, action) for rule in self._compiled)

    async def check_dag_access(self, request: Request, user: Any, method: str, entity: str | None) -> None:
        """403 ``Forbidden`` unless a rule allows the action of ``(method, entity)`` on the path's pipeline.

        The pipeline is the ``dag_id`` PATH parameter, as for Airflow; a request without one is
        denied too.
        """
        dag_id = request.path_params.get("dag_id")
        action = action_for(method, entity)
        if action is None or not self.allows(user, dag_id, action):
            log.debug("dag-healer: no RBAC rule allows %s %s on pipeline %r", method, entity, dag_id)
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=FORBIDDEN_DETAIL)

    def authorized_dag_ids(self, user: Any, method: str = "GET") -> set[str]:
        """The candidate pipelines ``user`` may read (``GET``) or act on (``PUT``).

        Any other method gives none, and so does a candidate listing that fails (logged).
        """
        action = action_for(method, "TASK_INSTANCE")
        if action is None:
            return set()
        try:
            candidates = set(self._candidates())
        except Exception as exc:  # e.g. an unreadable healer.yaml: fail closed, the page still loads
            log.warning(
                "dag-healer: the pipelines to authorize cannot be listed (%s); no pipeline is added",
                type(exc).__name__,
            )
            return set()
        return {pipeline_id for pipeline_id in candidates if self.allows(user, pipeline_id, action)}

    def can_read_dag(self, user: Any, dag_id: str) -> bool:
        """True when a rule lets ``user`` read ``dag_id``."""
        return self.allows(user, dag_id, READ)

    def can_edit_dag(self, user: Any, dag_id: str, entity: str | None = "TASK_INSTANCE") -> bool:
        """True when a rule allows the action of ``PUT entity`` on ``dag_id`` (``None``: the pipeline)."""
        action = action_for("PUT", entity)
        return action is not None and self.allows(user, dag_id, action)


__all__ = [
    "ACT",
    "ACTIONS",
    "APPROVE_CONTRACT",
    "DEV_RULES",
    "FORBIDDEN_DETAIL",
    "READ",
    "RuleAuthorizer",
    "action_for",
    "effective_rules",
]
