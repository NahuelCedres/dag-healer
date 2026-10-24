# dag-healer

**dag-healer** is an Apache Airflow 3 plugin that watches the task failures of your
*existing* DAGs, diagnoses them and acts on them. It does not create pipelines: you
`pip install dag-healer` into the Airflow environment you already run.

Every failure is classified as one of:

- **DATA** - an approved data contract was violated: dag-healer reports the violated
  rule with a masked sample of the offending rows and sends an alert.
- **CODE** - the data is fine and the traceback points at your code: dag-healer writes a
  pytest test that reproduces the bug, proposes a minimal fix, runs your test-suite and
  opens a GitHub pull request (or writes a `PR.md` + patch when no token is configured).
- **AMBIGUOUS** - a human decides from the dag-healer views inside the Airflow UI, and
  the agent resumes from there.

It integrates with Airflow through a plugin (`fastapi_apps`, `react_apps` and a task
listener), so the UI lives inside Airflow and respects its users and permissions.

> Status: early development (0.1.0). Requires Airflow >= 3.1 (developed against 3.3.2)
> and Python >= 3.10.

## Quickstart

TODO: installation into an existing Airflow deployment, `dag-healer init`, contract
approval and the end-to-end demo (`python scripts/tasks.py demo`).

## Example cases

- `example_existing_repo/` is a small legacy Airflow repository whose day-2 files break three
  DAGs, one per category; `python scripts/tasks.py demo` plays it end to end.
- `examples/synthetic/` adds three more pipelines with larger, fully synthetic datasets
  (`python scripts/synthetic_cases.py` after the demo). See its
  [README](examples/synthetic/README.md).

## Development

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -e ".[dev]"   # Linux/macOS: .venv/bin/python
.venv/Scripts/python -m pytest
.venv/Scripts/python -m ruff check .
```

Building a wheel requires the UI bundle (`src/dag_healer/api/static/main.umd.cjs`);
build it with `python scripts/tasks.py ui`, or set `DAG_HEALER_ALLOW_NO_UI=1` to build
a wheel without the Airflow UI views.

## Roadmap

[docs/roadmap.md](docs/roadmap.md) lists where dag-healer is going and in which order.

## Contributing

Contributions are welcome. [CONTRIBUTING.md](CONTRIBUTING.md) covers the development
setup, the checks to run and the commit convention
([Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)).

## Security

To report a vulnerability, see [SECURITY.md](SECURITY.md).
[docs/security.md](docs/security.md) describes the threat model, the known gaps and how to
deploy dag-healer until they are closed.

## License

Apache License 2.0 - see [LICENSE](LICENSE).
