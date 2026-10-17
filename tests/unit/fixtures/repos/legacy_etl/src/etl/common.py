# ruff: noqa
from pathlib import Path

AUDIT = Path(__file__).resolve().parents[2] / "data" / "audit.json"


def audit():
    return _write_audit()


def _write_audit():
    AUDIT.write_text("{}")
