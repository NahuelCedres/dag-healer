#!/usr/bin/env python3
"""End-to-end demo of dag-healer on the local Docker stack.

``python scripts/tasks.py demo`` (or ``make demo``, or ``python scripts/demo.py``) plays the
"day 2" story of ``example_existing_repo/``:

1. **reset** - stops the healer worker and recreates ``.demo/repo`` from
   ``example_existing_repo/`` (new git history, commit "Initial legacy repo");
2. **stack** - ``docker compose up --detach --wait`` (the image is built when it is missing,
   or always with ``--build``);
3. **adoption** - in the api-server, ``dag-healer store migrate`` (also with ``--no-reset``:
   a store written by an older image is upgraded before anything writes to it) and
   ``dag-healer store reset --yes --artifacts`` (empties the store; skipped with
   ``--no-reset``); then ``dag-healer init --repo /opt/airflow/repo``, demo settings in the
   generated ``healer.yaml`` (poll every 5 s, alerts to Mailpit ``mailpit:1025`` for
   ``data-team@example.com``), approval of the ``sales_daily_etl`` and ``customers_sync``
   contracts (``inventory_report`` stays a draft) and a commit "Add dag-healer config and
   contracts" in ``.demo/repo``; then the worker is started and ``dag-healer doctor`` runs in
   it: the demo stops early when its sandbox check fails, because the CODE repair runs every
   pytest run in the test sandbox, a fresh container of the stack image (no network, a
   throw-away copy of the clone) that the worker starts through the engine socket;
4. **day 2** - copies ``fixtures/day2/*.csv`` over ``data/incoming/`` (left uncommitted, like
   files dropped by an upstream team), triggers the three DAGs through the REST API and waits
   for their runs to fail;
5. **healing** - waits until the healer API reports the three incidents in their resting
   states: DATA ``reported``, CODE ``repaired`` (test + fix + PR.md) and AMBIGUOUS
   ``waiting_human``;
6. **summary** - prints the incidents, the UI / Mailpit URLs and copies each PR.md /
   fix.patch to ``.demo/prs/<incident_id>/`` on the host.

Options: ``--no-reset`` (keep the repository, the store and the configuration), ``--build``
(rebuild the image first), ``--listener-only`` (``monitor.modes: [listener]``: only the
Airflow listener creates incidents, the REST poll is off), ``--timeout`` (seconds to wait for
the incidents, default 600). Exit code 0 when the three incidents end as expected.

Standard library only; works on Windows, macOS and Linux (paths with spaces included).
Without ``ANTHROPIC_API_KEY`` the worker uses the offline "fake" LLM provider, which replays
``example_existing_repo/fixtures/fake_llm``.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import tasks  # noqa: E402  (scripts/tasks.py: docker compose helpers)

ROOT = tasks.ROOT
DEMO_DIR = tasks.DEMO_DIR
DEMO_REPO = tasks.DEMO_REPO
EXAMPLE_REPO = tasks.EXAMPLE_REPO
DEMO_PRS = DEMO_DIR / "prs"
DAY2_DIR = EXAMPLE_REPO / "fixtures" / "day2"

AIRFLOW_URL = tasks.AIRFLOW_URL
HEALER_API = "/healer/api/v1"
MAILPIT_URL = "http://localhost:8025"
APISERVER = tasks.APISERVER
WORKER = tasks.WORKER_SERVICE
CONTAINER_REPO = tasks.CONTAINER_REPO
CONTAINER_HEALER_HOME = "/opt/airflow/healer"
DEFAULT_IMAGE = "dag-healer/airflow:dev"

DEFAULT_TIMEOUT = 600
RUN_TIMEOUT = 300
DAG_READY_TIMEOUT = 300
POLL_SECONDS = 3.0
HTTP_TIMEOUT = 30.0

DEMO_APPROVER = "demo"
CONFIG_COMMIT_MESSAGE = "Add dag-healer config and contracts"
POLL_INTERVAL_SECONDS = 5
ALERT_HOST = "mailpit"
ALERT_PORT = 1025
ALERT_FROM = "dag-healer@example.com"
ALERT_TO = "data-team@example.com"
CSRF_HEADERS = {"X-Requested-With": "dag-healer"}
#: Name of the test-sandbox check in the ``dag-healer doctor`` output.
SANDBOX_CHECK = "sandbox"
#: Where to look when the sandbox check fails (the stack's socket mount and group).
SANDBOX_HELP = (
    "see 'Test sandbox' in docker/README.md (on Linux, HEALER_DOCKER_GID in docker/.env must be "
    "the group of the engine socket)"
)

#: Statuses in which an incident waits for nothing but a human (or nothing at all).
RESTING_STATUSES = frozenset({"reported", "repaired", "repair_failed", "waiting_human", "ignored", "error"})


@dataclass(frozen=True)
class Case:
    """One failure of the day-2 story and the outcome dag-healer must reach."""

    dag_id: str
    task_id: str
    category: str
    status: str
    approve: bool
    story: str


CASES: tuple[Case, ...] = (
    Case("sales_daily_etl", "transform_sales", "data", "reported", True, "amount renamed to amt upstream"),
    Case("customers_sync", "transform", "code", "repaired", True, "latent bug in parse_signup_date"),
    Case("inventory_report", "build_report", "ambiguous", "waiting_human", False, "qty -> quantity, draft"),
)


class DemoError(Exception):
    """The demo cannot continue; the message is shown to the user."""


def say(message: str) -> None:
    """Print a progress line of the demo."""
    print(f"[demo] {message}", flush=True)


def utcnow() -> datetime:
    """The current time (UTC, timezone-aware)."""
    return datetime.now(timezone.utc)


def parse_time(value: Any) -> datetime | None:
    """Parse an ISO-8601 timestamp as returned by the APIs (``Z`` suffix accepted)."""
    if not isinstance(value, str) or not value:
        return None
    text = value.strip().replace("Z", "+00:00")
    # Python < 3.11 only parses 3 or 6 fractional digits; the APIs send 1 to 9.
    text = re.sub(r"\.(\d+)", lambda m: "." + m.group(1)[:6].ljust(6, "0"), text, count=1)
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        return None
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


# --------------------------------------------------------------------------- HTTP


@dataclass
class Response:
    """An HTTP response (errors included: the caller checks ``status``)."""

    status: int
    text: str
    headers: dict[str, str] = field(default_factory=dict)

    def json(self) -> Any:
        """The body parsed as JSON (``None`` for an empty body)."""
        if not self.text.strip():
            return None
        try:
            return json.loads(self.text)
        except ValueError as exc:
            raise DemoError(f"expected JSON, got: {self.text[:200]!r}") from exc

    @property
    def detail(self) -> str:
        """The ``detail`` of an error body, else the start of the body."""
        try:
            body = json.loads(self.text)
        except ValueError:
            return self.text[:300]
        if isinstance(body, dict) and "detail" in body:
            return str(body["detail"])[:300]
        return self.text[:300]


_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def http(
    method: str,
    url: str,
    *,
    json_body: Any = None,
    headers: Mapping[str, str] | None = None,
    timeout: float = HTTP_TIMEOUT,
) -> Response:
    """Send one request (never through a proxy: everything is on localhost)."""
    data = None
    all_headers = {"Accept": "application/json", **(headers or {})}
    if json_body is not None:
        data = json.dumps(json_body).encode("utf-8")
        all_headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, method=method.upper(), headers=all_headers)
    try:
        with _OPENER.open(request, timeout=timeout) as reply:
            body = reply.read().decode("utf-8", errors="replace")
            return Response(reply.status, body, dict(reply.headers.items()))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if exc.fp is not None else ""
        return Response(exc.code, body, dict(exc.headers.items()) if exc.headers else {})
    except (urllib.error.URLError, OSError) as exc:
        reason = getattr(exc, "reason", exc)
        raise DemoError(f"{method.upper()} {url} failed: {reason}") from exc


def quote(segment: Any) -> str:
    """URL-quote one path segment (run ids contain ':' and '+')."""
    return urllib.parse.quote(str(segment), safe="")


class AirflowSession:
    """Talks to the Airflow REST API and the healer API as one Airflow user (JWT bearer)."""

    def __init__(
        self,
        username: str,
        password: str,
        *,
        base_url: str = AIRFLOW_URL,
        transport: Callable[..., Response] = http,
    ) -> None:
        self.username = username
        self._password = password
        self.base_url = base_url.rstrip("/")
        self._transport = transport
        self._token: str | None = None

    def login(self) -> str:
        """``POST /auth/token`` and cache the access token."""
        reply = self._transport(
            "POST",
            f"{self.base_url}/auth/token",
            json_body={"username": self.username, "password": self._password},
        )
        if reply.status not in (200, 201):
            raise DemoError(f"login as {self.username!r} failed: HTTP {reply.status} {reply.detail}")
        body = reply.json()
        token = body.get("access_token") if isinstance(body, dict) else None
        if not token:
            raise DemoError(f"login as {self.username!r} returned no access_token")
        self._token = str(token)
        return self._token

    @property
    def token(self) -> str:
        """The cached access token (logs in on first use)."""
        return self._token or self.login()

    def call(
        self,
        method: str,
        path: str,
        *,
        json_body: Any = None,
        headers: Mapping[str, str] | None = None,
        auth: bool = True,
        csrf: bool = True,
        timeout: float = HTTP_TIMEOUT,
    ) -> Response:
        """One request to ``base_url + path``; logs in again once on a 401."""
        for attempt in (1, 2):
            all_headers: dict[str, str] = {}
            if auth:
                all_headers["Authorization"] = f"Bearer {self.token}"
            if csrf and method.upper() != "GET":
                all_headers.update(CSRF_HEADERS)
            all_headers.update(headers or {})
            reply = self._transport(
                method, f"{self.base_url}{path}", json_body=json_body, headers=all_headers, timeout=timeout
            )
            if reply.status == 401 and auth and attempt == 1:
                self._token = None
                continue
            return reply
        return reply  # pragma: no cover - the loop always returns

    def healer(self, method: str, path: str, **kwargs: Any) -> Response:
        """A request to the healer API (``/healer/api/v1`` + ``path``)."""
        return self.call(method, f"{HEALER_API}{path}", **kwargs)

    def ok(self, method: str, path: str, *, expect: Iterable[int] = (200,), **kwargs: Any) -> Any:
        """A request that must answer one of ``expect``; returns the JSON body."""
        reply = self.call(method, path, **kwargs)
        expected = tuple(expect)
        if reply.status not in expected:
            raise DemoError(f"{method.upper()} {path} answered HTTP {reply.status}: {reply.detail}")
        return reply.json()


# --------------------------------------------------------------------------- docker compose


def compose(*args: str, check: bool = True) -> int:
    """``docker compose <args>`` for the dag-healer project (output streamed)."""
    code = tasks.run(tasks.compose_base() + list(args))
    if check and code != 0:
        raise DemoError(f"docker compose {' '.join(args)} failed with exit code {code}")
    return code


def compose_exec(service: str, command: Sequence[str], *, check: bool = True) -> tuple[int, str]:
    """Run ``command`` in a running service container; returns ``(code, output)``."""
    cmd = tasks.compose_base() + ["exec", "-T", service, *command]
    print(f"$ {tasks.display_command(cmd)}", flush=True)
    code, out = tasks.capture(cmd)
    if out.strip():
        print(out.rstrip(), flush=True)
    if check and code != 0:
        raise DemoError(f"'{' '.join(command)}' failed in {service} with exit code {code}")
    return code, out


def image_name() -> str:
    """The stack's image (``HEALER_IMAGE`` from the shell or docker/.env, else the default)."""
    if os.environ.get("HEALER_IMAGE"):
        return os.environ["HEALER_IMAGE"]
    try:
        text = tasks.ENV_FILE.read_text(encoding="utf-8")
    except OSError:
        return DEFAULT_IMAGE
    match = re.search(r"(?m)^HEALER_IMAGE=(\S+)\s*$", text)
    return match.group(1) if match else DEFAULT_IMAGE


def image_exists(docker: str, name: str) -> bool:
    """True when the image exists locally."""
    code, _ = tasks.capture([docker, "image", "inspect", "--format", "{{.Id}}", name], timeout=60)
    return code == 0


# --------------------------------------------------------------------------- test sandbox

_DOCTOR_LINE = re.compile(r"^\[\s*(?P<label>[A-Z]+)\s*\] (?P<rest>.*)$")


def doctor_check(output: str, name: str = SANDBOX_CHECK) -> tuple[str, str] | None:
    """``(label, detail)`` of one check in the output of ``dag-healer doctor`` (None: absent).

    A doctor line reads ``[ OK ] <name, padded>  <detail>``; the label is ``OK``, ``WARN``,
    ``FAIL`` or ``SKIP``.
    """
    for line in output.splitlines():
        match = _DOCTOR_LINE.match(line.strip())
        if match is None:
            continue
        rest = match.group("rest")
        if rest == name or rest.startswith(f"{name} "):
            return match.group("label"), rest[len(name) :].strip()
    return None


def check_sandbox(service: str = WORKER) -> tuple[str, str]:
    """Run ``dag-healer doctor`` in ``service``; returns its sandbox check as ``(label, detail)``.

    The CODE repair runs every pytest run in the test sandbox, which the worker starts through
    the engine socket. With a failing sandbox check that repair would end ``repair_failed``
    without running any code, so this raises DemoError with the doctor's reason instead. The
    other checks do not stop the demo: what they report shows up in its later steps.
    """
    code, out = compose_exec(service, ["dag-healer", "doctor"], check=False)
    found = doctor_check(out)
    if found is None:
        raise DemoError(
            f"'dag-healer doctor' in {service} printed no {SANDBOX_CHECK} check (exit code {code}): "
            "is the image older than the test sandbox? Rerun with --build"
        )
    label, detail = found
    if label == "FAIL":
        raise DemoError(f"the test sandbox does not work in {service}: {detail}; {SANDBOX_HELP}")
    return label, detail


# --------------------------------------------------------------------------- demo repository


def _remove(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        tasks.remove_tree(path)
    else:
        path.unlink()


def refresh_demo_repo(example: Path = EXAMPLE_REPO, repo: Path = DEMO_REPO) -> Path:
    """Make ``repo`` a fresh git copy of ``example`` (one commit, "Initial legacy repo").

    ``repo`` is bind-mounted into the running containers, so the directory itself is kept and
    ``dags/`` is never removed: its files are overwritten in place (the DAG processor never
    sees the DAGs disappear). Everything else - git history, healer.yaml, contracts, job
    outputs, tests and branches written by earlier runs - is deleted first.
    """
    if not example.is_dir():
        raise DemoError(f"{tasks.rel(example)} does not exist")
    git = tasks.which("git", "Install git from https://git-scm.com/downloads.")
    repo.mkdir(parents=True, exist_ok=True)
    for child in list(repo.iterdir()):
        if child.name != "dags" or not child.is_dir():
            _remove(child)
    dags = repo / "dags"
    wanted = {path.name for path in (example / "dags").iterdir()} if (example / "dags").is_dir() else set()
    if dags.is_dir():
        for child in list(dags.iterdir()):
            if child.name not in wanted or child.name == "__pycache__":
                _remove(child)
    try:
        shutil.copytree(example, repo, dirs_exist_ok=True, ignore=tasks.COPY_IGNORE)
    except (OSError, shutil.Error) as exc:
        raise DemoError(f"could not copy {tasks.rel(example)} to {tasks.rel(repo)}: {exc}") from exc
    dags.mkdir(exist_ok=True)
    tasks.init_git_repo(git, repo)
    return repo


def git_out(repo: Path, *args: str) -> str:
    """Output of a git command in ``repo`` (raises DemoError on failure)."""
    git = tasks.which("git", "Install git from https://git-scm.com/downloads.")
    code, out = tasks.capture([git, "-C", str(repo), *args])
    if code != 0:
        raise DemoError(f"git {' '.join(args)} failed in {tasks.rel(repo)}:\n{out.strip()}")
    return out.strip()


def commit_config(repo: Path = DEMO_REPO) -> bool:
    """Commit healer.yaml and the contracts; returns False when there was nothing to commit."""
    paths = [name for name in ("healer.yaml", "healer") if (repo / name).exists()]
    if not paths:
        raise DemoError(f"{tasks.rel(repo)} has no healer.yaml: did 'dag-healer init' run?")
    git_out(repo, "add", "--", *paths)
    if not git_out(repo, "diff", "--cached", "--name-only"):
        return False
    git_out(repo, "commit", "-q", "-m", CONFIG_COMMIT_MESSAGE)
    return True


def healer_branches(repo: Path = DEMO_REPO) -> list[str]:
    """The ``healer/*`` branches pushed into the demo repository by the worker."""
    out = git_out(repo, "branch", "--list", "healer/*", "--format=%(refname:short)")
    return [line.strip() for line in out.splitlines() if line.strip()]


def copy_day2(repo: Path = DEMO_REPO, day2: Path = DAY2_DIR) -> list[str]:
    """Copy the day-2 CSV files over ``data/incoming`` (left uncommitted)."""
    incoming = repo / "data" / "incoming"
    incoming.mkdir(parents=True, exist_ok=True)
    copied = []
    for source in sorted(day2.glob("*.csv")):
        shutil.copyfile(source, incoming / source.name)
        copied.append(source.name)
    if not copied:
        raise DemoError(f"no day-2 files in {tasks.rel(day2)}")
    return copied


# --------------------------------------------------------------------------- healer.yaml


def _replace_line(text: str, pattern: str, replacement: str, what: str) -> str:
    new_text, count = re.subn(pattern, replacement, text, count=1, flags=re.MULTILINE)
    if count != 1:
        raise DemoError(f"healer.yaml: cannot find {what} to set it for the demo")
    return new_text


def configure_healer_yaml(text: str, *, listener_only: bool = False) -> str:
    """Apply the demo settings to a ``healer.yaml`` generated by ``dag-healer init``.

    Poll every 5 s; ``monitor.modes`` ``[listener]`` with ``listener_only`` else
    ``[rest, listener]``; e-mail alerts to Mailpit (``mailpit:1025``, no TLS, no login) for
    ``data-team@example.com``. Comments are kept; applying it twice gives the same text.
    """
    modes = "[listener]" if listener_only else "[rest, listener]"
    text = _replace_line(
        text,
        r"^([ \t]+poll_interval_seconds:)[ \t]*[^\s#]+([ \t]*(?:#.*)?)$",
        rf"\g<1> {POLL_INTERVAL_SECONDS}\2",
        "monitor.poll_interval_seconds",
    )
    text = _replace_line(text, r"^([ \t]+modes:)[ \t]*\[[^\]\n]*\]", rf"\g<1> {modes}", "monitor.modes")
    email = {
        "host": ALERT_HOST,
        "port": str(ALERT_PORT),
        "from": json.dumps(ALERT_FROM),
        "to": json.dumps([ALERT_TO]),
        "starttls": "false",
        "username": "null",
        "password": "null",
    }
    for key, value in email.items():
        text = _replace_line(
            text,
            rf"^(    {key}:)[ \t]*(?:\"[^\"\n]*\"|\[[^\]\n]*\]|[^\s#]+)([ \t]*(?:#.*)?)$",
            rf"\g<1> {value}\2",
            f"notify.email.{key}",
        )
    marker = "# Demo settings (scripts/demo.py): poll every 5 s, alerts to Mailpit (mailpit:1025)."
    if marker not in text:
        text = f"{marker}\n{text}"
    return text


def write_demo_config(repo: Path = DEMO_REPO, *, listener_only: bool = False) -> Path:
    """Rewrite ``<repo>/healer.yaml`` with :func:`configure_healer_yaml` (keeps LF endings)."""
    path = repo / "healer.yaml"
    try:
        original = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise DemoError(f"cannot read {tasks.rel(path)}: {exc}") from exc
    updated = configure_healer_yaml(original, listener_only=listener_only)
    if updated != original:
        with path.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(updated)
    return path


# --------------------------------------------------------------------------- Airflow


def wait_for_dag(api: AirflowSession, dag_id: str, timeout: float = DAG_READY_TIMEOUT) -> dict[str, Any]:
    """Wait until Airflow has parsed ``dag_id`` (not stale, no import errors); unpause it."""
    deadline = time.monotonic() + timeout
    last = ""
    while True:
        reply = api.call("GET", f"/api/v2/dags/{quote(dag_id)}")
        if reply.status == 200:
            dag = reply.json() or {}
            if dag.get("has_import_errors"):
                last = "it has import errors"
            elif dag.get("is_stale"):
                last = "it is stale (its file is being parsed again)"
            else:
                if dag.get("is_paused"):
                    say(f"unpausing {dag_id}")
                    api.ok("PATCH", f"/api/v2/dags/{quote(dag_id)}", json_body={"is_paused": False})
                return dag
        else:
            last = f"HTTP {reply.status} {reply.detail}"
        if time.monotonic() >= deadline:
            raise DemoError(f"DAG {dag_id} is not ready after {int(timeout)} s: {last}")
        time.sleep(POLL_SECONDS)


def trigger_dag(api: AirflowSession, dag_id: str) -> str:
    """Trigger a manual run of ``dag_id``; returns its run id."""
    body = api.ok(
        "POST",
        f"/api/v2/dags/{quote(dag_id)}/dagRuns",
        json_body={"logical_date": None, "conf": {}},
        expect=(200, 201),
    )
    run_id = (body or {}).get("dag_run_id")
    if not run_id:
        raise DemoError(f"triggering {dag_id} returned no dag_run_id")
    return str(run_id)


def wait_for_runs(
    api: AirflowSession, runs: Mapping[str, str], timeout: float = RUN_TIMEOUT
) -> dict[str, str]:
    """Wait until every run is ``failed`` or ``success``; returns ``dag_id -> state``."""
    deadline = time.monotonic() + timeout
    states: dict[str, str] = {}
    while True:
        for dag_id, run_id in runs.items():
            if states.get(dag_id) in ("failed", "success"):
                continue
            body = api.ok("GET", f"/api/v2/dags/{quote(dag_id)}/dagRuns/{quote(run_id)}")
            state = str((body or {}).get("state") or "unknown")
            if state != states.get(dag_id):
                say(f"run of {dag_id}: {state}")
            states[dag_id] = state
        if all(states.get(dag_id) in ("failed", "success") for dag_id in runs):
            return states
        if time.monotonic() >= deadline:
            raise DemoError(f"DAG runs did not finish within {int(timeout)} s: {states}")
        time.sleep(POLL_SECONDS)


# --------------------------------------------------------------------------- healer API


def incidents_of_runs(
    incidents: Iterable[Mapping[str, Any]],
    runs: Mapping[str, str],
    *,
    since: datetime | None = None,
) -> dict[str, dict]:
    """The newest incident of each triggered run (``dag_id -> IncidentSummary``).

    With ``since``, a DAG whose run has no incident of its own falls back to its newest
    incident updated after ``since``: dag-healer folds a repeated failure into the incident
    that is still open for the same error (``occurrences`` grows, no new incident).
    """
    found: dict[str, dict] = {}
    folded: dict[str, dict] = {}
    for incident in incidents:
        dag_id = str(incident.get("dag_id"))
        if dag_id not in runs:
            continue
        if runs[dag_id] == incident.get("run_id"):
            current = found.get(dag_id)
            if current is None or str(incident.get("created_at") or "") > str(
                current.get("created_at") or ""
            ):
                found[dag_id] = dict(incident)
        elif since is not None and (parse_time(incident.get("updated_at")) or since) > since:
            current = folded.get(dag_id)
            if current is None or str(incident.get("updated_at") or "") > str(
                current.get("updated_at") or ""
            ):
                folded[dag_id] = dict(incident)
    for dag_id, incident in folded.items():
        found.setdefault(dag_id, incident)
    return found


def wait_for_incidents(
    api: AirflowSession,
    runs: Mapping[str, str],
    timeout: float = DEFAULT_TIMEOUT,
    *,
    since: datetime | None = None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, dict]:
    """Poll the healer API until the incident of every run is in a resting status.

    ``since`` (when the runs were triggered) lets a failure folded into an older open
    incident count for its run (see :func:`incidents_of_runs`).
    """
    deadline = clock() + timeout
    seen: dict[str, str] = {}
    while True:
        body = api.ok("GET", f"{HEALER_API}/incidents?limit=200")
        found = incidents_of_runs((body or {}).get("incidents") or [], runs, since=since)
        for dag_id, incident in sorted(found.items()):
            label = f"{incident.get('status')} ({incident.get('category') or 'unclassified'})"
            if seen.get(dag_id) != label:
                say(f"incident of {dag_id}.{incident.get('task_id')}: {label}")
                seen[dag_id] = label
        if len(found) == len(runs) and all(i.get("status") in RESTING_STATUSES for i in found.values()):
            return found
        if clock() >= deadline:
            missing = sorted(set(runs) - set(found))
            raise DemoError(
                f"timed out after {int(timeout)} s waiting for the incidents "
                f"(seen: {seen or 'none'}; missing: {missing or 'none'}). "
                "Worker logs: python scripts/tasks.py logs healer-worker"
            )
        sleep(POLL_SECONDS)


def check_outcomes(found: Mapping[str, Mapping[str, Any]], cases: Sequence[Case] = CASES) -> list[str]:
    """Differences between the incidents and the story's expected outcomes (empty: all good)."""
    problems = []
    for case in cases:
        incident = found.get(case.dag_id)
        if incident is None:
            problems.append(f"{case.dag_id}: no incident")
            continue
        got = (incident.get("task_id"), incident.get("category"), incident.get("status"))
        want = (case.task_id, case.category, case.status)
        if got != want:
            problems.append(
                f"{case.dag_id}: expected task/category/status {'/'.join(want)}, "
                f"got {'/'.join(str(part) for part in got)}"
            )
    return problems


def copy_pr_artifacts(
    api: AirflowSession, incident: Mapping[str, Any], dest_root: Path = DEMO_PRS
) -> Path | None:
    """Copy ``PR.md`` and ``fix.patch`` of an incident to ``.demo/prs/<id>/`` on the host."""
    incident_id = str(incident["id"])
    dest = dest_root / incident_id
    dest.mkdir(parents=True, exist_ok=True)
    source = f"{APISERVER}:{CONTAINER_HEALER_HOME}/prs/{incident_id}/."
    cmd = tasks.compose_base() + ["cp", source, str(dest)]
    code, out = tasks.capture(cmd, timeout=120)
    if code != 0 or not (dest / "PR.md").is_file():
        # fall back to the API (PR.md only)
        reply = api.healer("GET", f"/dags/{quote(incident['dag_id'])}/incidents/{quote(incident_id)}/pr.md")
        if reply.status != 200:
            say(f"could not copy the PR.md of {incident_id}: {out.strip() or reply.detail}")
            return None
        (dest / "PR.md").write_text(reply.text, encoding="utf-8", newline="\n")
    return dest / "PR.md"


# --------------------------------------------------------------------------- the demo


@dataclass
class DemoOptions:
    """Options of one demo run (see the module docstring)."""

    reset: bool = True
    build: bool = False
    listener_only: bool = False
    timeout: float = DEFAULT_TIMEOUT
    username: str = "admin"
    password: str = "admin"


@dataclass
class DemoResult:
    """What a demo run produced (used by scripts/e2e_smoke.py)."""

    started_at: datetime
    runs: dict[str, str]
    incidents: dict[str, dict]
    details: dict[str, dict]
    pr_files: dict[str, Path]
    branches: list[str]
    problems: list[str]

    @property
    def ok(self) -> bool:
        """True when the three incidents ended as the story expects."""
        return not self.problems


def start_stack(options: DemoOptions) -> None:
    """Build the image when needed and start every service except the worker."""
    docker = tasks.ensure_docker_running()
    tasks.ensure_env_file()
    name = image_name()
    if options.build or not image_exists(docker, name):
        say(f"building {name} (UI bundle + wheel + Airflow; the first build takes several minutes)")
        compose("build")
    compose("up", "--detach", "--wait", "--wait-timeout", str(tasks.DEFAULT_TIMEOUT), *tasks.CORE_SERVICES)
    if not tasks.wait_for_airflow(tasks.DEFAULT_TIMEOUT):
        raise DemoError("Airflow did not become healthy; see: python scripts/tasks.py logs airflow-apiserver")


def prepare_store(reset: bool) -> None:
    """Upgrade the healer store to the image's schema, then empty it when ``reset``.

    ``store migrate`` always runs first: ``store reset`` and the contract approvals of
    :func:`adopt` write the store without migrating it, so a store left by an older image
    would make them fail (only the worker, ``dag-healer init`` and ``store migrate`` migrate).
    """
    compose_exec(APISERVER, ["dag-healer", "store", "migrate"])
    if reset:
        compose_exec(APISERVER, ["dag-healer", "store", "reset", "--yes", "--artifacts"])


def adopt(options: DemoOptions) -> None:
    """``dag-healer init``, demo settings, contract approvals and the config commit."""
    compose_exec(APISERVER, ["dag-healer", "init", "--repo", CONTAINER_REPO])
    write_demo_config(listener_only=options.listener_only)
    modes = "[listener]" if options.listener_only else "[rest, listener]"
    alerts = f"{ALERT_HOST}:{ALERT_PORT}"
    say(f"healer.yaml: poll every {POLL_INTERVAL_SECONDS} s, modes {modes}, alerts to {alerts}")
    for case in CASES:
        if case.approve:
            compose_exec(
                APISERVER, ["dag-healer", "contracts", "approve", case.dag_id, "--by", DEMO_APPROVER]
            )
    if commit_config():
        say(f"committed healer.yaml and contracts in {tasks.rel(DEMO_REPO)} ('{CONFIG_COMMIT_MESSAGE}')")


def run_demo(options: DemoOptions) -> DemoResult:
    """Run the whole demo; raises DemoError when a step cannot be completed."""
    started_at = utcnow()
    tasks.ensure_docker_running()
    tasks.ensure_env_file()

    if options.reset:
        say("1/6 reset: stopping the worker and recreating .demo/repo from example_existing_repo/")
        compose("stop", WORKER, check=False)
        refresh_demo_repo()
        tasks.DEMO_LOGS.mkdir(parents=True, exist_ok=True)
        if DEMO_PRS.exists():
            tasks.remove_tree(DEMO_PRS)
    else:
        say("1/6 reset skipped (--no-reset)")
        tasks.prepare_demo_repo()

    say("2/6 starting the stack (docker compose up --wait)")
    start_stack(options)

    say("3/6 adopting dag-healer in the repository")
    prepare_store(options.reset)
    adopt(options)
    compose("up", "--detach", "--wait", "--wait-timeout", "300", WORKER)
    label, detail = check_sandbox()
    say(f"test sandbox [{label}]: {detail}")

    say("4/6 day 2: new files in data/incoming/, triggering the DAGs")
    copied = copy_day2()
    say(f"copied fixtures/day2/{{{','.join(copied)}}} to {tasks.rel(DEMO_REPO)}/data/incoming/ (uncommitted)")
    api = AirflowSession(options.username, options.password)
    runs: dict[str, str] = {}
    triggered_at = utcnow() - timedelta(seconds=5)
    for case in CASES:
        wait_for_dag(api, case.dag_id)
        runs[case.dag_id] = trigger_dag(api, case.dag_id)
        say(f"triggered {case.dag_id}: {runs[case.dag_id]}")
    states = wait_for_runs(api, runs)
    succeeded = sorted(dag_id for dag_id, state in states.items() if state != "failed")
    if succeeded:
        raise DemoError(f"these runs did not fail on the day-2 files: {', '.join(succeeded)}")

    say(f"5/6 waiting for dag-healer (up to {int(options.timeout)} s)")
    found = wait_for_incidents(api, runs, options.timeout, since=triggered_at)

    say("6/6 summary")
    details: dict[str, dict] = {}
    pr_files: dict[str, Path] = {}
    for dag_id, incident in found.items():
        details[dag_id] = api.ok(
            "GET", f"{HEALER_API}/dags/{quote(dag_id)}/incidents/{quote(incident['id'])}"
        )
        if incident.get("has_pr_md"):
            copied_pr = copy_pr_artifacts(api, incident)
            if copied_pr is not None:
                pr_files[dag_id] = copied_pr
    return DemoResult(
        started_at=started_at,
        runs=runs,
        incidents=found,
        details=details,
        pr_files=pr_files,
        branches=healer_branches(),
        problems=check_outcomes(found),
    )


def print_summary(result: DemoResult) -> None:
    """The incidents table, the URLs and where the PR.md files are."""
    rows = []
    for case in CASES:
        incident = result.incidents.get(case.dag_id, {})
        pr = incident.get("pr_url") or (
            tasks.rel(result.pr_files[case.dag_id]) if case.dag_id in result.pr_files else "-"
        )
        rows.append(
            (
                case.dag_id,
                incident.get("task_id", "-"),
                incident.get("category") or "-",
                incident.get("status", "-"),
                incident.get("source", "-"),
                pr,
            )
        )
    headers = ("DAG", "TASK", "CATEGORY", "STATUS", "SOURCE", "PR / PR.md")
    widths = [max(len(str(row[i])) for row in [headers, *rows]) for i in range(len(headers))]
    print()
    for row in [headers, tuple("-" * w for w in widths), *rows]:
        cells = (str(cell).ljust(width) for cell, width in zip(row, widths, strict=True))
        print("  " + "  ".join(cells).rstrip())
    print()
    print(f"  Healer page ............ {AIRFLOW_URL}/plugin/healer   (admin/admin, viewer/viewer)")
    for case in CASES:
        links = (result.details.get(case.dag_id) or {}).get("links") or {}
        if links.get("task_instance"):
            print(f"  {case.dag_id + ' ':.<23} {links['task_instance']}")
    print(f"  Mailpit (alerts) ....... {MAILPIT_URL}")
    for dag_id, path in result.pr_files.items():
        print(f"  PR.md ({dag_id}) {path}")
    if result.branches:
        print(f"  Branches in .demo/repo . {', '.join(result.branches)}")
    waiting = [c.dag_id for c in CASES if result.incidents.get(c.dag_id, {}).get("status") == "waiting_human"]
    if waiting:
        print()
        print(
            f"  {', '.join(waiting)} waits for a human: open its 'Healer incident' tab and choose "
            "'Resolve as data' or 'Resolve as code'."
        )
    print()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Command line of the demo."""
    parser = argparse.ArgumentParser(
        prog="python scripts/demo.py",
        description="Run the dag-healer day-2 demo on the Docker stack (see the module docstring).",
    )
    parser.add_argument("--no-reset", action="store_true", help="keep .demo/repo, the store and healer.yaml")
    parser.add_argument("--build", action="store_true", help="rebuild the image before starting the stack")
    parser.add_argument(
        "--listener-only",
        action="store_true",
        help="monitor.modes: [listener] (only the Airflow listener creates incidents)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        help="seconds to wait for the incidents (default 600)",
    )
    parser.add_argument("--user", default="admin", help="Airflow user that triggers the DAGs (default admin)")
    parser.add_argument("--password", default="admin", help="its password (default admin)")
    return parser.parse_args(argv)


def options_from_args(args: argparse.Namespace) -> DemoOptions:
    """Turn the parsed command line into :class:`DemoOptions`."""
    return DemoOptions(
        reset=not args.no_reset,
        build=args.build,
        listener_only=args.listener_only,
        timeout=args.timeout,
        username=args.user,
        password=args.password,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point; returns the exit code."""
    args = parse_args(argv)
    try:
        result = run_demo(options_from_args(args))
    except (DemoError, tasks.TaskError) as exc:
        print(f"[demo] error: {exc}", file=sys.stderr, flush=True)
        return 1
    except KeyboardInterrupt:
        print("[demo] interrupted", file=sys.stderr, flush=True)
        return 130
    except subprocess.SubprocessError as exc:
        print(f"[demo] error: {exc}", file=sys.stderr, flush=True)
        return 1
    print_summary(result)
    if result.problems:
        for problem in result.problems:
            print(f"[demo] unexpected outcome: {problem}", file=sys.stderr, flush=True)
        return 1
    say("done: DATA reported, CODE repaired, AMBIGUOUS waiting for a human")
    return 0


if __name__ == "__main__":
    sys.exit(main())
