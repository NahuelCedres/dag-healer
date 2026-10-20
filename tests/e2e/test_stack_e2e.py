"""End-to-end smoke test against the Docker stack (opt-in: set ``HEALER_E2E=1``).

It runs ``scripts/e2e_smoke.py``: the demo with a reset (``.demo/repo`` recreated, the store
emptied, ``dag-healer init``, two contracts approved, the day-2 files dropped, the three DAGs
triggered), then the checks of the three incidents (DATA reported, CODE repaired with PR.md,
AMBIGUOUS waiting for a human), the healer/* branch, the Mailpit alerts, the API security
(401 / 403) and the human-in-the-loop flow (resolve as code, resolve as data). It needs Docker
Desktop / Docker Engine and takes several minutes::

    HEALER_E2E=1 python -m pytest tests/e2e -q
    HEALER_E2E=1 HEALER_E2E_ARGS="--listener-only" python -m pytest tests/e2e -q
    python scripts/tasks.py e2e            # the same checks without pytest

``tests/e2e`` is not in the default ``testpaths`` (``tests/unit``), so a plain ``pytest`` run
never starts containers.
"""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "e2e_smoke.py"
TIMEOUT_SECONDS = 3600

pytestmark = pytest.mark.skipif(
    os.environ.get("HEALER_E2E") != "1",
    reason="set HEALER_E2E=1 to run the end-to-end smoke test against the Docker stack",
)


def test_e2e_smoke_against_the_docker_stack() -> None:
    extra = shlex.split(os.environ.get("HEALER_E2E_ARGS", ""))
    proc = subprocess.run(
        [sys.executable, str(SCRIPT), *extra], cwd=ROOT, timeout=TIMEOUT_SECONDS, check=False
    )
    assert proc.returncode == 0, "scripts/e2e_smoke.py reported failed checks (see the [e2e] FAIL lines)"
