"""Entry point of the ``dag-healer`` command.

Commands live in a registry so other modules can contribute them without touching
the parser code::

    from dag_healer.cli.main import register_command

    def _configure(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--repo", default=".")

    def _run(args: argparse.Namespace) -> int:
        ...
        return 0

    register_command("init", help="Discover DAGs and write draft contracts.",
                     handler=_run, configure=_configure)

Modules listed in :data:`COMMAND_MODULES` are imported before the parser is built,
so registering a new command module is a one-line change there. Command modules
should keep heavy imports (pandas, SQLAlchemy, ...) inside their handlers so that
``dag-healer version`` and ``--help`` stay fast. argparse is used on purpose: it adds
no dependency that could conflict with Airflow's own pins.
"""

from __future__ import annotations

import argparse
import importlib
import logging
import os
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path

from dag_healer._version import __version__

log = logging.getLogger(__name__)

Handler = Callable[[argparse.Namespace], int | None]
Configure = Callable[[argparse.ArgumentParser], None]

#: Modules that register commands on import (they keep heavy imports inside handlers).
COMMAND_MODULES: tuple[str, ...] = (
    "dag_healer.cli.init_cmd",
    "dag_healer.cli.contracts_cmd",
    "dag_healer.cli.incidents_cmd",
    "dag_healer.cli.doctor_cmd",
    "dag_healer.cli.worker_cmd",
    "dag_healer.cli.store_cmd",
    "dag_healer.cli.orchestrators_cmd",
    "dag_healer.cli.serve_cmd",
)


@dataclass(frozen=True)
class Command:
    """A registered sub-command."""

    name: str
    help: str
    handler: Handler
    configure: Configure | None = None
    aliases: tuple[str, ...] = ()


COMMANDS: dict[str, Command] = {}


def register_command(
    name: str,
    *,
    help: str,
    handler: Handler,
    configure: Configure | None = None,
    aliases: Sequence[str] = (),
) -> Command:
    """Register (or replace) the sub-command ``name``."""
    command = Command(name=name, help=help, handler=handler, configure=configure, aliases=tuple(aliases))
    COMMANDS[name] = command
    return command


def _load_command_modules() -> None:
    for module_name in COMMAND_MODULES:
        try:
            importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            if exc.name != module_name:
                raise
            log.debug("dag-healer: command module %s is not available", module_name)


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser from the command registry."""
    _load_command_modules()
    parser = argparse.ArgumentParser(
        prog="dag-healer",
        description="Diagnose and repair failing tasks of existing Airflow DAGs.",
    )
    parser.add_argument("--version", action="version", version=f"dag-healer {__version__}")
    parser.add_argument(
        "-c",
        "--config",
        metavar="PATH",
        default=None,
        help="Path to healer.yaml (default: $HEALER_CONFIG, $AIRFLOW_HOME/healer.yaml, ./healer.yaml). "
        "Exported as HEALER_CONFIG for the rest of the process.",
    )
    parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="Increase log verbosity (-v info, -vv debug)."
    )
    subparsers = parser.add_subparsers(dest="command", metavar="<command>")
    for command in sorted(COMMANDS.values(), key=lambda c: c.name):
        sub = subparsers.add_parser(
            command.name, help=command.help, description=command.help, aliases=list(command.aliases)
        )
        if command.configure is not None:
            command.configure(sub)
        sub.set_defaults(_handler=command.handler)
    return parser


#: The anthropic SDK's DEBUG record of every request (``Request options: ...``) quotes the whole
#: request body: the prompts, with the masked evidence and the repository files they carry. Under
#: ``-vv`` it is dropped unless ``ANTHROPIC_LOG=debug`` asks for the SDK's own debug logs.
SDK_REQUEST_LOGGER = "anthropic._base_client"
SDK_REQUEST_RECORD = "Request options"
#: The SDK's tool runner logs the stop reason of a repair's last turn as the answer gave it, at
#: DEBUG (``Turn ended with stop_reason ...``): that record is dropped (the worker's own line at
#: the end of the session names the stop reason).
SDK_RUNNER_LOGGER = "anthropic.lib.tools._beta_runner"
SDK_RUNNER_RECORD = "Turn ended with stop_reason"
#: The loggers of the HTTP clients under the SDK (httpx2, and httpx for anthropic 1.x): their INFO
#: record of each request (``HTTP Request: POST <url> "HTTP/1.1 403 Forbidden"``) quotes the
#: status line of the answer, whose reason phrase the endpoint writes.
CLIENT_LOGGERS: tuple[str, ...] = ("httpx", "httpx2")
#: The loggers of the HTTP transports under the clients (httpcore2, and httpcore for httpx): their
#: DEBUG traces quote the status line and the raw headers of every answer, and the failure of a
#: request quotes the bytes that h11 refused (``Illegal header value b'...'``, ``illegal status
#: line: bytearray(b'...')``).
TRANSPORT_LOGGERS: tuple[str, ...] = tuple(
    f"{package}.{module}"
    for package in ("httpcore", "httpcore2")
    for module in ("connection", "http11", "http2", "proxy", "socks")
)
#: Every logger that the filter goes on, ahead of its creation (and on any other httpcore logger
#: that exists then).
FILTERED_LOGGERS: tuple[str, ...] = (
    SDK_REQUEST_LOGGER,
    SDK_RUNNER_LOGGER,
    *CLIENT_LOGGERS,
    *TRANSPORT_LOGGERS,
)
#: Stands for the headers of an answer in a record of the SDK or of a transport.
HEADERS_NOT_SHOWN = "[headers not shown]"
#: The SDK's records of one header of an answer (``request_id: %s``, ``workspace_id: %s``).
_SDK_HEADER_RECORDS = ("request_id:", "workspace_id:")
#: The records that quote the status line of an answer through their arguments: the SDK's
#: ``HTTP Response: %s %s "%i %s" %s`` and the clients' ``HTTP Request: %s %s "%s %d %s"``.
_STATUS_LINE_RECORDS = ("HTTP Response:", "HTTP Request:")
#: A transport's trace of the status line of an answer (``return_value=(b'HTTP/1.1', 403,
#: b'Forbidden', ...``): its reason phrase, a bytes literal in either quote, after the status code.
_TRACED_REASON = re.compile(
    r"(return_value=\(b'HTTP/[0-9.]+', ([0-9]{3}), )"  # the HTTP version and the status code
    r"b(?:'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\")"  # the reason phrase
)
#: A transport's list of raw headers (``[(b'Content-Type', b'application/json'), ...]``).
_RAW_HEADERS = re.compile(r"\[\(b['\"].*\)\]", re.S)
#: What a record names or quotes when it can show a credential: a header that carries one, by
#: its name; h11's quote of the bytes it refused (a header line, name or value, a status or a
#: request line, a chunk header or footer), which it prints as they came; httpx's
#: ``HTTPStatusError``, whose message quotes the status line of the answer (anthropic 1.x logs it
#: with its traceback).
_QUOTES_A_CREDENTIAL = re.compile(
    r"\b(?:authorization|x-api-key|x-amz-security-token)\b"
    r"|illegal (?:header|status line|request line|chunk header)|malformed chunk|\bHTTPStatusError\b",
    re.I,
)
#: The credential variables of the LLM providers (``dag_healer.agent.llm._CREDENTIAL_ENVS`` and
#: ``dag_healer.agent.bedrock._SECRET_ENVS``, which a test keeps equal to this list), spelled out
#: here so that the CLI imports neither provider.
_CREDENTIAL_ENVS = (
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
    "AWS_BEARER_TOKEN_BEDROCK",
)


class _SdkRequestFilter(logging.Filter):
    """Keeps the prompts, what an answer brings and the credentials out of the records of the
    anthropic SDK, of the HTTP clients under it and of their transports (:data:`FILTERED_LOGGERS`),
    at any level; the records of other loggers pass untouched.

    * The SDK's ``Request options`` records (the whole request body) are dropped, unless
      ``keep_requests`` (``ANTHROPIC_LOG=debug``, or a level above DEBUG), and so are its tool
      runner's ``Turn ended with stop_reason`` records.
    * What an answer brings that AWS, a gateway or a proxy can fill with an echo of a credential
      is replaced (:func:`_hide_answer`): its headers (an ``x-amzn-ErrorType``, any other
      header) by :data:`HEADERS_NOT_SHOWN`, in the SDK's ``HTTP Response`` record (any mapping
      among the arguments of its records), its ``request_id`` and ``workspace_id`` records, and
      the transports' traces of ``receive_response_headers``; its reason phrase by the standard
      one of its status code, in the SDK's ``HTTP Response`` record, the clients' ``HTTP
      Request`` record and the transports' traces.
    * A record whose text, its traceback included, names an ``Authorization``, ``x-api-key`` or
      ``x-amz-security-token`` header, quotes bytes that h11 refused or an ``HTTPStatusError``
      (:data:`_QUOTES_A_CREDENTIAL`), or holds the value of a credential variable
      (:data:`_CREDENTIAL_ENVS`) or a fragment of 16 characters of one, is dropped.
    """

    def __init__(self, *, keep_requests: bool = False) -> None:
        super().__init__()
        self.keep_requests = keep_requests

    def filter(self, record: logging.LogRecord) -> bool:
        if not record.name.startswith(("anthropic", "httpx", "httpcore")):
            return True
        message = record.msg if isinstance(record.msg, str) else ""
        if message.startswith(SDK_REQUEST_RECORD) and not self.keep_requests:
            return False
        if message.startswith(SDK_RUNNER_RECORD):
            return False
        _hide_answer(record)
        return not _holds_credential(_record_text(record))


def _standard_reason(status: int) -> str:
    """The standard reason phrase of an HTTP status code (``Forbidden`` for 403), ``""`` for an
    unknown code."""
    try:
        return HTTPStatus(status).phrase
    except ValueError:
        return ""


def _is_status(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 100 <= value <= 599


def _hide_answer(record: logging.LogRecord) -> None:
    """Replace what ``record`` quotes of an answer, in place: its headers by
    :data:`HEADERS_NOT_SHOWN`, its reason phrase by the standard one of its status code."""
    message = record.msg
    if isinstance(record.args, tuple) and record.args:
        if isinstance(message, str) and message.startswith(_SDK_HEADER_RECORDS):
            record.args = tuple(arg if arg is None else HEADERS_NOT_SHOWN for arg in record.args)
            return
        args = [HEADERS_NOT_SHOWN if isinstance(arg, Mapping) else arg for arg in record.args]
        if isinstance(message, str) and message.startswith(_STATUS_LINE_RECORDS):
            for index in range(len(args) - 1):  # the reason phrase follows the status code
                if _is_status(args[index]) and isinstance(args[index + 1], (str, bytes)):
                    args[index + 1] = _standard_reason(args[index])
        record.args = tuple(args)
    elif isinstance(message, str) and record.name.startswith("httpcore"):
        message = _TRACED_REASON.sub(_traced_standard_reason, message)
        record.msg = _RAW_HEADERS.sub(HEADERS_NOT_SHOWN, message)


def _traced_standard_reason(match: re.Match[str]) -> str:
    """The status line of :data:`_TRACED_REASON`'s ``match`` with the standard reason phrase."""
    return match.group(1) + repr(_standard_reason(int(match.group(2))).encode("ascii"))


def _record_text(record: logging.LogRecord) -> str:
    """What a handler would print of ``record``: its message, traceback and stack."""
    try:
        parts = [record.getMessage()]
    except Exception:  # a record whose arguments do not format: judge its format string
        parts = [str(record.msg)]
    if record.exc_info:
        parts.append(logging.Formatter().formatException(record.exc_info))
    elif record.exc_text:
        parts.append(record.exc_text)
    if record.stack_info:
        parts.append(record.stack_info)
    return "\n".join(parts)


def _holds_credential(text: str) -> bool:
    """True when ``text`` names a credential header, quotes bytes that h11 refused or an
    ``HTTPStatusError`` (:data:`_QUOTES_A_CREDENTIAL`), or holds the value of a variable of
    :data:`_CREDENTIAL_ENVS`, whole or as a fragment of 16 characters, as it is or with its
    whitespace taken out (:func:`dag_healer.masking.holds_secret`)."""
    if _QUOTES_A_CREDENTIAL.search(text):
        return True
    from dag_healer.masking import holds_secret, known_secrets

    secrets = known_secrets(os.environ.get(name) for name in _CREDENTIAL_ENVS)
    return bool(secrets) and (holds_secret(text, secrets) or holds_secret("".join(text.split()), secrets))


_SDK_FILTER = _SdkRequestFilter()


def _quiet_sdk_records(*, keep_requests: bool) -> None:
    """Install the one :class:`_SdkRequestFilter` on the loggers of :data:`FILTERED_LOGGERS` (made
    ahead of the libraries, by name) and any other httpcore logger that exists, idempotent."""
    _SDK_FILTER.keep_requests = keep_requests
    existing = [name for name in list(logging.Logger.manager.loggerDict) if name.startswith("httpcore")]
    for name in dict.fromkeys([*FILTERED_LOGGERS, *existing]):
        target = logging.getLogger(name)
        if not any(isinstance(item, _SdkRequestFilter) for item in target.filters):
            target.addFilter(_SDK_FILTER)


def _configure_logging(verbosity: int) -> None:
    level = logging.WARNING if verbosity <= 0 else logging.INFO if verbosity == 1 else logging.DEBUG
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    sdk_debug = os.environ.get("ANTHROPIC_LOG", "").strip().lower() == "debug"
    # At every level: ANTHROPIC_LOG=debug turns the SDK's DEBUG records on whatever -v says.
    _quiet_sdk_records(keep_requests=sdk_debug or level > logging.DEBUG)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI and return its exit code."""
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    _configure_logging(args.verbose)
    if args.config:
        os.environ["HEALER_CONFIG"] = str(Path(args.config).expanduser().resolve())
    handler: Handler | None = getattr(args, "_handler", None)
    if handler is None:
        parser.print_help(sys.stderr)
        return 2
    try:
        result = handler(args)
    except KeyboardInterrupt:
        return 130
    return 0 if result is None else int(result)


# --------------------------------------------------------------------------- built-ins


def _cmd_version(args: argparse.Namespace) -> int:
    print(f"dag-healer {__version__}")
    return 0


register_command("version", help="Show the dag-healer version.", handler=_cmd_version)


if __name__ == "__main__":
    # Re-import under the canonical name so commands registered by other modules
    # (which import dag_healer.cli.main) land in the same registry.
    from dag_healer.cli.main import main as _canonical_main

    raise SystemExit(_canonical_main())
