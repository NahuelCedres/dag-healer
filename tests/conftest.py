"""Shared pytest fixtures (keep this file tiny: piece-specific fixtures live next to their tests)."""

from __future__ import annotations

import textwrap
from collections.abc import Callable, Mapping
from pathlib import Path

import pytest

MakeRepo = Callable[..., Path]


def write_files(root: Path, files: Mapping[str, str | bytes]) -> Path:
    """Write ``{relative/posix/path: content}`` under ``root`` (str content is dedented, LF)."""
    for relpath, content in files.items():
        target = root / relpath
        target.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            target.write_bytes(content)
        else:
            target.write_text(textwrap.dedent(content), encoding="utf-8", newline="\n")
    return root


@pytest.fixture
def make_repo(tmp_path: Path) -> MakeRepo:
    """Factory creating a throw-away repository directory with the given files.

    ``make_repo({"dags/x.py": "...", "data/in.csv": "a,b\\n1,2\\n"}, name="repo")``
    returns the repository root. ``dags/`` and ``src/`` always exist.
    """

    def _make(files: Mapping[str, str | bytes] | None = None, *, name: str = "repo") -> Path:
        root = tmp_path / name
        (root / "dags").mkdir(parents=True, exist_ok=True)
        (root / "src").mkdir(parents=True, exist_ok=True)
        return write_files(root, files or {})

    return _make


@pytest.fixture
def tmp_repo(make_repo: MakeRepo) -> Path:
    """An empty repository layout (``dags/`` and ``src/``) in a temporary directory."""
    return make_repo()


#: Variables that change dag-healer's behaviour; ``healer_env`` unsets them for each test.
HEALER_ENV_VARS: tuple[str, ...] = (
    "HEALER_CONFIG",
    "HEALER_DATABASE_URL",
    "HEALER_API_URL",
    "HEALER_PUBLIC_URL",
    "HEALER_INGEST_TOKEN",
    "HEALER_DEBUG",
    "HEALER_LLM_PROVIDER",
    "HEALER_FAKE_LLM_DIR",
    "HEALER_AIRFLOW_USERNAME",
    "HEALER_AIRFLOW_PASSWORD",
    "HEALER_SMTP_HOST",
    "HEALER_SANDBOX_IMAGE",
    "AIRFLOW_HOME",
    "AIRFLOW__CORE__EXECUTION_API_SERVER_URL",
    "AIRFLOW__CORE__DAGS_FOLDER",
    "AIRFLOW__API__BASE_URL",
    "GITHUB_TOKEN",
    "GITHUB_REPO",
)


@pytest.fixture
def healer_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Isolate dag-healer from the host environment; returns a temporary HEALER_HOME.

    Each variable is first set, then deleted, through ``monkeypatch``: that records its
    original state, so whatever the code under test writes to ``os.environ`` directly (for
    example ``main(["-c", ...])`` exporting ``HEALER_CONFIG``) is undone at teardown and
    never leaks into the next test.
    """
    for name in HEALER_ENV_VARS:
        monkeypatch.setenv(name, "")
        monkeypatch.delenv(name)
    home = tmp_path / "healer_home"
    home.mkdir()
    monkeypatch.setenv("HEALER_HOME", str(home))
    return home
