"""Console deep links: the :class:`~dag_healer.core.ports.NativeLinks` (section 9.11).

The URL formats are not documented AWS contracts [U]: they are the templates of
``options.links`` (defaults in :mod:`dag_healer.stepfunctions.options`). Placeholders
(``{region}``, ``{account}``, ``{partition}``, ``{execution_arn}``, ``{execution_name}``,
``{state_machine_arn}``, ``{state_machine_name}``, ``{log_group}``, ``{log_group_enc}``) are
quoted with ``quote(value, safe=":")``, so ids with ``/``, ``+``, spaces or ``#`` cannot break
the URL and ``unquote`` gives them back. ``{log_group_enc}`` is the CloudWatch console encoding
of a log group (``/aws/lambda/fn`` -> ``$252Faws$252Flambda$252Ffn``): it is URL-safe already
and inserted as it is.

``links(ref, native)`` never does I/O (the API process calls it for every incident) and never
raises:

* ``"AWS console"``: the execution page (``native.execution_arn``, else built from the
  pipeline's known state machine ARN and ``ref.run_id``, an ``@<ms>`` reuse suffix dropped unless
  ``native.start_date`` shows it is part of the name), or the state machine page;
* ``"CloudWatch logs"``: the Lambda log group (``native.log_group``, else
  ``/aws/lambda/<native.function_name>``) when known.

The region is ``options.region``, else the one of the ARN. A link whose template needs a value
that is not known is left out (an empty mapping when nothing is known).
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote, urlsplit

from dag_healer.core.ids import AttemptRef
from dag_healer.stepfunctions import arns
from dag_healer.stepfunctions.runtime import SfnRuntime, epoch_ms, native_text

CONSOLE_LABEL = "AWS console"
LOGS_LABEL = "CloudWatch logs"
#: Prefix of the default log group of a Lambda function.
LAMBDA_LOG_GROUP_PREFIX = "/aws/lambda/"
#: Placeholders whose values are already encoded for the URL (inserted without quoting).
PREENCODED_PLACEHOLDERS = frozenset({"log_group_enc"})

_PLACEHOLDER = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
_FUNCTION_NAME = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def render_template(template: str, values: Mapping[str, str]) -> str:
    """Fill a link template; every value is quoted with ``quote(value, safe=":")``, except the
    already encoded :data:`PREENCODED_PLACEHOLDERS` (``{log_group_enc}``).

    Raises :class:`KeyError` for a placeholder ``values`` does not provide (``None`` and empty
    values count as missing). Text outside ``{name}`` placeholders is kept as it is.
    """

    def fill(match: re.Match[str]) -> str:
        name = match.group(1)
        value = values.get(name)
        if value is None or str(value) == "":
            raise KeyError(name)
        text = str(value)
        return text if name in PREENCODED_PLACEHOLDERS else quote(text, safe=":")

    return _PLACEHOLDER.sub(fill, template)


def log_group_console_encoding(log_group: str) -> str:
    """The CloudWatch console encoding of a log group name (``/`` -> ``$252F`` ...): the name
    percent-encoded with nothing safe, then every ``%`` written as ``$25``."""
    return quote(log_group, safe="").replace("%", "$25")


def lambda_log_group(function: str) -> str | None:
    """``/aws/lambda/<name>`` of a Lambda function ARN, partial ARN or (qualified) name; ``None``
    when ``function`` is not a usable function name (an unresolved ``${Placeholder}``, another ARN)."""
    try:
        name = arns.lambda_function_name(function)
    except ValueError:
        return None
    return f"{LAMBDA_LOG_GROUP_PREFIX}{name}" if _FUNCTION_NAME.match(name) else None


def _parse_execution(text: str | None) -> arns.ExecutionArn | None:
    if text is None:
        return None
    try:
        return arns.parse_execution_arn(text)
    except ValueError:
        return None


def _parse_machine(text: str | None) -> arns.StateMachineArn | None:
    if text is None or not arns.is_arn(text):
        return None
    try:
        return arns.parse_state_machine_arn(text)
    except ValueError:
        return None


def _function_region(function: str | None) -> str | None:
    """The region of a Lambda function ARN (``None`` for a name or anything else)."""
    if function is None or not arns.is_arn(function):
        return None
    try:
        return arns.parse_arn(function).region or None
    except ValueError:
        return None


def _absolute(url: str) -> bool:
    parts = urlsplit(url)
    return parts.scheme in ("http", "https") and bool(parts.netloc)


class SfnLinks:
    """``NativeLinks`` port (a separate object: ``adapter.links.links(...)``)."""

    def __init__(self, rt: SfnRuntime) -> None:
        self.rt = rt

    def links(self, ref: AttemptRef, native: Mapping[str, Any]) -> dict[str, str]:
        """``{"AWS console": ..., "CloudWatch logs": ...}`` (see the module docstring); no I/O."""
        values = self.placeholder_values(ref, native)
        templates = self.rt.options.links
        found: dict[str, str] = {}
        console = None
        if "execution_arn" in values:
            console = self._render(templates.console_execution_url, values)
        if console is None and "state_machine_arn" in values:
            console = self._render(templates.console_state_machine_url, values)
        if console is not None:
            found[CONSOLE_LABEL] = console
        if "log_group" in values:
            logs = self._render(templates.cloudwatch_log_url, values)
            if logs is not None:
                found[LOGS_LABEL] = logs
        return found

    def placeholder_values(self, ref: AttemptRef, native: Mapping[str, Any]) -> dict[str, str]:
        """The placeholder values known for ``(ref, native)`` without I/O (missing ones left out)."""
        data = native if isinstance(native, Mapping) else {}
        execution = _parse_execution(native_text(data, "execution_arn"))
        machine = _parse_machine(native_text(data, "state_machine_arn"))
        if machine is None and execution is None:
            machine = _parse_machine(self.rt.pipelines.known_arn(ref.pipeline_id))
        if execution is None and machine is not None and not machine.is_map_run_child and ref.run_id:
            name = arns.execution_name(ref.run_id, start=epoch_ms(data.get("start_date")))
            try:
                built = arns.execution_arn(machine.unqualified, name)
            except ValueError:
                built = None
            execution = _parse_execution(built)
        values: dict[str, str] = {}
        source: arns.ExecutionArn | arns.StateMachineArn | None = execution or machine
        if execution is not None:
            values["execution_arn"] = str(execution)
            values["execution_name"] = execution.name
            values["state_machine_arn"] = execution.state_machine_arn
            values["state_machine_name"] = execution.state_machine
        elif machine is not None:
            values["state_machine_arn"] = machine.unqualified
            values["state_machine_name"] = machine.name
        function = native_text(data, "function_name")
        region = self.rt.options.region or (source.region if source is not None else None)
        region = region or _function_region(function)
        if region:
            values["region"] = region
        if source is not None:
            if source.account:
                values["account"] = source.account
            values["partition"] = source.partition
        elif region:
            values["partition"] = arns.partition_for_region(region)
        log_group = native_text(data, "log_group")
        if log_group is None and function is not None:
            log_group = lambda_log_group(function)
        if log_group:
            values["log_group"] = log_group
            values["log_group_enc"] = log_group_console_encoding(log_group)
        return values

    @staticmethod
    def _render(template: str, values: Mapping[str, str]) -> str | None:
        try:
            url = render_template(template, values)
        except KeyError:
            return None
        return url if _absolute(url) else None


__all__ = [
    "CONSOLE_LABEL",
    "LAMBDA_LOG_GROUP_PREFIX",
    "LOGS_LABEL",
    "PREENCODED_PLACEHOLDERS",
    "SfnLinks",
    "lambda_log_group",
    "log_group_console_encoding",
    "render_template",
]
