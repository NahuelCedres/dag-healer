"""Opt-in tests of the container sandbox against a real engine (docker, or podman).

Skipped unless ``HEALER_SANDBOX_TEST_IMAGE`` names an image with Python and pytest, for example
the one built from ``tests/sandbox``::

    docker build -t dag-healer/sandbox-test:dev tests/sandbox
    HEALER_SANDBOX_TEST_IMAGE=dag-healer/sandbox-test:dev \\
        python -m pytest -q -p no:cacheprovider tests/unit/test_sandbox_engine.py

``HEALER_SANDBOX_TEST_ENGINE`` selects ``podman`` instead of ``docker``. The assertions hold for
any such image (the Docker demo stack's ``dag-healer/airflow:dev`` too). The containers are small
and every test checks that none of its containers is left behind.
"""

from __future__ import annotations

import json
import os
import subprocess
import textwrap
import time
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from dag_healer.repo import sandbox as sandbox_module
from dag_healer.repo.pytest_runner import run_pytest
from dag_healer.repo.sandbox import (
    CONTAINER_LABEL,
    KILLED_EXIT_CODE,
    LAUNCH_FAILED_EXIT_CODE,
    TIMEOUT_EXIT_CODE,
    ContainerSandbox,
)

IMAGE = os.environ.get("HEALER_SANDBOX_TEST_IMAGE")
ENGINE = os.environ.get("HEALER_SANDBOX_TEST_ENGINE", "docker")

pytestmark = [
    pytest.mark.skipif(
        not IMAGE, reason="set HEALER_SANDBOX_TEST_IMAGE to an image with Python and pytest (tests/sandbox)"
    ),
    pytest.mark.skipif(
        os.name != "posix", reason="the real-engine tests create symbolic links on a POSIX host"
    ),
]

CANARY = "dag-healer-canary-7d1f0c"

FACTS_TEST = """
    import importlib.util
    import json
    import os
    import socket

    PROXIES = ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy")


    def _writable(path):
        try:
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("x")
            os.remove(path)
            return True
        except OSError:
            return False


    def _connect():
        try:
            socket.create_connection(("1.1.1.1", 53), timeout=3).close()
            return "open"
        except OSError as exc:
            return "blocked: %s" % exc


    def _environ_holding(value):
        holders = []
        for entry in os.listdir("/proc"):
            if entry.isdigit():
                try:
                    with open("/proc/%s/environ" % entry, "rb") as handle:
                        if value.encode() in handle.read():
                            holders.append(entry)
                except OSError:
                    pass
        return holders


    def test_facts():
        pytest_dir = os.path.dirname(importlib.util.find_spec("pytest").origin)
        facts = {
            "euid": os.geteuid(),
            "interfaces": sorted(os.listdir("/sys/class/net")),
            "connect": _connect(),
            "root_writable": _writable("/dag-healer-probe"),
            "site_packages_writable": _writable(os.path.join(pytest_dir, "dag-healer-probe")),
            "work_writable": _writable("dag-healer-probe"),
            "tmp_writable": _writable("/tmp/dag-healer-probe"),
            "cwd": os.getcwd(),
            "home": os.environ.get("HOME"),
            "tmpdir": os.environ.get("TMPDIR"),
            "airflow_home": os.environ.get("AIRFLOW_HOME"),
            "git": os.path.lexists(".git"),
            "links": [name for name in ("absolute_link", "escaping_link") if os.path.lexists(name)],
            "inside_link": os.path.islink("inside_link") and os.readlink("inside_link"),
            "canary_in_environ": sorted(k for k, v in os.environ.items() if "@CANARY@" in v),
            "canary_in_proc": _environ_holding("@CANARY@"),
            "engine_variables": sorted(
                k for k in os.environ if k.upper().startswith(("DOCKER_", "CONTAINER_", "CONTAINERS_"))
            ),
            "proxies": [os.environ.get(k) for k in PROXIES],
        }
        print("FACTS " + json.dumps(facts))
""".replace("@CANARY@", CANARY)

PROJECT = {
    "pytest.ini": "[pytest]\n",
    "README.md": "readme\n",
    "pkg/__init__.py": "",
    "pkg/calc.py": "def double(x):\n    return 2 * x\n",
    "tests/test_ok.py": """
        from pkg.calc import double


        def test_double():
            assert double(2) == 4
    """,
    "tests/test_bad.py": """
        from pkg.calc import double


        def test_double_negative():
            assert double(-1) == 2
    """,
    "tests/test_vandal.py": """
        import os
        import pathlib


        def test_vandal():
            root = pathlib.Path.cwd()
            (root / "created.txt").write_text("x", encoding="utf-8")
            os.remove(root / "README.md")
            (root / ".git" / "hooks").mkdir(parents=True)
            (root / ".git" / "hooks" / "pre-commit").write_text("#!/bin/sh\\n", encoding="utf-8")
    """,
    "tests/test_facts.py": FACTS_TEST,
    "slow/test_sleep.py": """
        import time


        def test_sleeps():
            time.sleep(120)
    """,
    "flood/test_flood.py": """
        import sys


        def test_floods():
            line = "x" * 1023 + "\\n"
            while True:
                sys.stdout.write(line)
    """,
    ".git/HEAD": "ref: refs/heads/main\n",
    ".git/config": "[core]\n\tbare = false\n",
}


def settings(**overrides: Any) -> SimpleNamespace:
    """Small limits: the tests run on developer machines and CI runners."""
    values: dict[str, Any] = {
        "backend": "container",
        "engine": ENGINE,
        "image": IMAGE,
        "python": "python",
        "user": None,
        "memory": "256m",
        "cpus": 1.0,
        "pids": 128,
        "work_size": "64m",
        "tmp_size": "64m",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def sandbox_containers() -> set[str]:
    listed = subprocess.run(
        [ENGINE, "ps", "-a", "--filter", f"label={CONTAINER_LABEL}=1", "--format", "{{.Names}}"],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    return {line.strip() for line in listed.stdout.splitlines() if line.strip()}


@pytest.fixture(autouse=True)
def no_container_left_behind() -> Iterator[None]:
    before = sandbox_containers()
    yield
    deadline = time.monotonic() + 20
    while (left := sandbox_containers() - before) and time.monotonic() < deadline:
        time.sleep(0.5)
    assert left == set()


def snapshot(root: Path) -> dict[str, bytes]:
    """Every entry under ``root``, links not followed (a link's target, a file's content)."""
    found: dict[str, bytes] = {}
    for dirpath, dirnames, filenames in os.walk(root):
        for name in [*dirnames, *filenames]:
            path = Path(dirpath) / name
            if path.is_symlink():
                content = os.readlink(path).encode()
            else:
                content = b"" if path.is_dir() else path.read_bytes()
            found[path.relative_to(root).as_posix()] = content
    return found


@pytest.fixture
def project(tmp_path: Path) -> Path:
    root = tmp_path / "clone"
    for rel, content in PROJECT.items():
        target = root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(textwrap.dedent(content).lstrip("\n"), encoding="utf-8", newline="\n")
    (tmp_path / "outside.txt").write_text(CANARY, encoding="utf-8")
    os.symlink("pkg/calc.py", root / "inside_link")
    os.symlink(str(tmp_path / "outside.txt"), root / "absolute_link")
    os.symlink("../outside.txt", root / "escaping_link")
    return root


@pytest.fixture(scope="module")
def sandbox() -> ContainerSandbox:
    return ContainerSandbox(settings())


def test_the_image_is_usable_and_the_smoke_test_proves_the_isolation(sandbox: ContainerSandbox) -> None:
    assert sandbox.unavailable_reason() is None
    probe = sandbox.smoke_test()
    assert probe.ok, probe.detail
    assert f"image {IMAGE}: uid " in probe.detail and "no network (interfaces: lo)" in probe.detail


def test_counts_of_a_run(sandbox: ContainerSandbox, project: Path) -> None:
    result = run_pytest(project, ["tests/test_ok.py", "tests/test_bad.py"], timeout=120, sandbox=sandbox)
    assert (result.exit_code, result.passed, result.failed, result.launch_error) == (1, 1, 1, None), (
        result.output_tail
    )
    assert "test_double_negative" in result.output_tail


def test_inside_the_container(
    sandbox: ContainerSandbox, project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SANDBOX_TEST_API_KEY", CANARY)  # a worker secret (scrubbed by name)
    monkeypatch.setenv("SANDBOX_TEST_SETTING", f"harmless-{CANARY}")  # passed on: proves the probe sees env
    monkeypatch.setenv("HTTP_PROXY", f"http://user:{CANARY}@proxy.invalid:3128")
    monkeypatch.setenv("https_proxy", f"http://user:{CANARY}@proxy.invalid:3128")
    before = snapshot(project)

    result = run_pytest(project, ["tests/test_facts.py"], timeout=120, extra_args=["-s"], sandbox=sandbox)

    assert result.ok, result.output_tail
    line = next(line for line in result.output_tail.splitlines() if line.startswith("FACTS "))
    facts = json.loads(line[len("FACTS ") :])
    assert facts["euid"] != 0
    assert facts["interfaces"] == ["lo"] and facts["connect"].startswith("blocked: ")
    assert facts["root_writable"] is False and facts["site_packages_writable"] is False
    assert facts["work_writable"] is True and facts["tmp_writable"] is True
    assert (facts["cwd"], facts["home"], facts["tmpdir"], facts["airflow_home"]) == (
        "/work",
        "/tmp/home",
        "/tmp",
        "/tmp/airflow_home",
    )
    assert facts["git"] is False and facts["links"] == [] and facts["inside_link"] == "pkg/calc.py"
    assert facts["canary_in_environ"] == ["SANDBOX_TEST_SETTING"]  # only the harmless one
    assert len(facts["canary_in_proc"]) >= 1  # the probe sees environments: its own, never the worker's
    assert facts["engine_variables"] == []
    assert set(facts["proxies"]) <= {"", None}
    assert snapshot(project) == before


def test_the_worker_environment_never_reaches_the_container(
    sandbox: ContainerSandbox, project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SANDBOX_TEST_API_KEY", CANARY)
    result = run_pytest(project, ["tests/test_facts.py"], timeout=120, extra_args=["-s"], sandbox=sandbox)
    assert result.ok, result.output_tail
    line = next(line for line in result.output_tail.splitlines() if line.startswith("FACTS "))
    facts = json.loads(line[len("FACTS ") :])
    assert facts["canary_in_environ"] == [] and facts["canary_in_proc"] == []


def test_the_clone_never_changes(sandbox: ContainerSandbox, project: Path) -> None:
    before = snapshot(project)
    result = run_pytest(project, ["tests/test_vandal.py"], timeout=120, sandbox=sandbox)
    assert result.ok, result.output_tail
    assert snapshot(project) == before


def test_the_timeout_kills_the_container(sandbox: ContainerSandbox, project: Path) -> None:
    start = time.monotonic()
    result = run_pytest(project, ["slow/test_sleep.py"], timeout=8, sandbox=sandbox)
    assert result.timed_out and result.exit_code == TIMEOUT_EXIT_CODE
    assert time.monotonic() - start < 60
    # no_container_left_behind checks that the container is gone


def test_a_flood_of_output_kills_the_container(
    sandbox: ContainerSandbox, project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sandbox_module, "MAX_OUTPUT_BYTES", 8 * 1024**2)
    start = time.monotonic()
    result = run_pytest(project, ["flood/test_flood.py"], timeout=120, extra_args=["-s"], sandbox=sandbox)
    assert (result.exit_code, result.timed_out, result.launch_error) == (KILLED_EXIT_CODE, False, None)
    assert time.monotonic() - start < 60
    assert result.output_tail.endswith(
        "pytest was killed: its output passed 8 MiB (only the end of it is kept)\n"
    )
    # no_container_left_behind checks that the container is gone


def test_a_missing_image(project: Path) -> None:
    missing = ContainerSandbox(settings(image="dag-healer/sandbox-test:no-such-tag"))
    reason = missing.unavailable_reason()
    assert reason is not None and "is not present on the" in reason and "never pulls images" in reason
    result = run_pytest(project, ["tests/test_ok.py"], timeout=120, sandbox=missing)
    assert result.exit_code == LAUNCH_FAILED_EXIT_CODE
    assert result.launch_error is not None and "is not present on the" in result.launch_error


def test_a_root_user_is_refused_before_and_inside_the_container(project: Path) -> None:
    root = ContainerSandbox(settings(user="0"))
    assert (
        root.unavailable_reason()
        == "repo.sandbox.user '0' is root; use a non-root user, or null for the image's USER"
    )
    result = run_pytest(project, ["tests/test_ok.py"], timeout=120, sandbox=root)
    assert result.exit_code == LAUNCH_FAILED_EXIT_CODE and "is root" in (result.launch_error or "")

    # Past the settings check, the bootstrap refuses uid 0 inside the container.
    root._settings_problem = lambda python=None: None  # type: ignore[method-assign]
    result = run_pytest(project, ["tests/test_ok.py"], timeout=120, sandbox=root)
    assert result.exit_code == LAUNCH_FAILED_EXIT_CODE
    assert (result.launch_error or "").startswith("refusing to run as root (uid 0)")
    assert result.passed == 0
