"""Tests of ``dag-healer orchestrators list``."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dag_healer.cli.main import COMMANDS, build_parser, main
from dag_healer.orchestrators import registry
from dag_healer.orchestrators.registry import Registration


@pytest.fixture(autouse=True)
def _isolated(healer_env: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """No healer.yaml in the current directory, the default type is whatever the host has."""
    monkeypatch.chdir(tmp_path)


def _registered(monkeypatch: pytest.MonkeyPatch, *items: Registration) -> None:
    monkeypatch.setattr(registry, "_entry_point_registrations", lambda: list(items))
    registry.reset_adapters()


def test_the_command_is_registered() -> None:
    build_parser()
    assert "orchestrators" in COMMANDS


def test_list_prints_every_type_with_its_status(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setitem(registry.BUILTIN_TYPES, "ghost", "dag_healer.no_such_adapter:FACTORY")
    _registered(monkeypatch)
    assert main(["orchestrators", "list"]) == 0
    out = capsys.readouterr().out
    lines = out.splitlines()
    assert lines[0].split() == ["TYPE", "DISPLAY", "NAME", "PROVIDED", "BY", "FACTORY", "STATUS"]
    fake_row = next(line for line in lines if line.startswith("fake "))
    assert "Fakeflow" in fake_row and "dag-healer (built-in)" in fake_row and fake_row.endswith("available")
    ghost_row = next(line for line in lines if line.startswith("ghost "))
    assert ghost_row.endswith("unavailable")
    assert "\nghost: module 'dag_healer.no_such_adapter' is not installed" in out
    assert {"airflow", "fake", "ghost", "stepfunctions"} <= {line.split()[0] for line in lines[2:] if line}
    assert "Configured: orchestrator.type airflow (instance airflow; no healer.yaml found, defaults)" in out


def test_list_as_json(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["orchestrators", "list", "--json"]) == 0
    data = json.loads(capsys.readouterr().out)
    rows = {row["type"]: row for row in data["types"]}
    assert [row["type"] for row in data["types"]] == sorted(rows)
    assert rows["fake"] == {
        "type": "fake",
        "display_name": "Fakeflow",
        "distribution": rows["fake"]["distribution"],
        "target": "dag_healer.orchestrators.fake:FACTORY",
        "status": "available",
        "detail": None,
        "secret_options": ["orchestrator.options.token"],
    }
    assert rows["fake"]["distribution"].startswith("dag-healer")
    assert data["configured"] == {"type": "airflow", "instance": "airflow", "source": None}


def test_list_shows_the_configured_file(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    config = tmp_path / "healer.yaml"
    config.write_text("version: 1\n", encoding="utf-8")
    assert main(["orchestrators", "list", "-c", str(config)]) == 0
    assert (
        f"Configured: orchestrator.type airflow (instance airflow; {config.resolve()})"
        in capsys.readouterr().out
    )


def test_an_invalid_configuration_is_a_warning(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    config = tmp_path / "healer.yaml"
    config.write_text("version: 7\n", encoding="utf-8")
    assert main(["-c", str(config), "orchestrators", "list"]) == 0
    captured = capsys.readouterr()
    assert "warning: cannot read the configured orchestrator" in captured.err
    assert "unsupported config version 7" in captured.err
    assert "fake " in captured.out and "Configured:" not in captured.out


def test_a_conflict_is_listed_and_fails_the_command(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _registered(monkeypatch, Registration("fake", "acme.fake:FACTORY", "acme-fake"))
    assert main(["orchestrators", "list"]) == 1
    out = capsys.readouterr().out
    fake_row = next(line for line in out.splitlines() if line.startswith("fake "))
    assert fake_row.endswith("conflict") and "acme-fake" in fake_row and "dag-healer (built-in)" in fake_row
    assert "is registered more than once" in out


def test_without_an_action_the_help_is_printed(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["orchestrators"]) == 2
    assert "list" in capsys.readouterr().out
