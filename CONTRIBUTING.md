# Contributing to dag-healer

Thanks for your interest in dag-healer. The project is in early development: until 1.0,
expect `healer.yaml`, the store schema and the HTTP API to change between minor versions.

## Ground rules

- dag-healer reads data that may be sensitive. Never paste raw rows, real credentials or
  unmasked logs into issues, pull requests, fixtures or tests. Use synthetic data, like
  the files under `example_existing_repo/`.
- Secrets only come from environment variables (`${ENV}` references in `healer.yaml`).
  Never commit a literal token, password or API key.
- Report security vulnerabilities privately (see [Security issues](#security-issues)),
  not in a public issue.

## Development setup

You need Python 3.10+, Git and Docker with Compose v2. Node.js is optional: the UI bundle
can be built inside a `node:22-alpine` container.

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -e ".[dev]"   # Linux/macOS: .venv/bin/python
```

Everything that needs Airflow runs in Docker. `scripts/tasks.py` wraps the common tasks
and works without `make` (every `make <target>` delegates to it):

| Command | What it does |
|---|---|
| `python scripts/tasks.py up` | build the image if needed, start Airflow 3.3.2 and the healer worker |
| `python scripts/tasks.py demo` | scripted end-to-end demo with three failures: DATA, CODE and AMBIGUOUS |
| `python scripts/tasks.py ui` | build the React bundles of the Airflow plugin and of `dag-healer serve` (local Node >= 20.19, otherwise Docker) |
| `python scripts/tasks.py test` | unit tests, no Airflow needed |
| `python scripts/tasks.py test-airflow` | tests that need Airflow, in the `tests` compose profile |
| `python scripts/tasks.py e2e` | smoke test against the running stack |
| `python scripts/tasks.py doctor` | `dag-healer doctor` in the worker's container, including the test sandbox check |
| `python scripts/tasks.py down` | stop the stack |
| `python scripts/tasks.py --help` | every command |

## Checks before a pull request

```bash
.venv/Scripts/python -m pytest -q
.venv/Scripts/python -m ruff check src tests scripts
.venv/Scripts/python -m ruff format --check src tests scripts
```

- UI changes: `pnpm lint`, `pnpm format:check` and `pnpm test` in `ui/`.
- Plugin, API or worker changes: `python scripts/tasks.py test-airflow` as well.
- Test sandbox changes (`repo.sandbox`, `src/dag_healer/repo/sandbox.py`): the opt-in tests
  against a real container engine as well. They need Docker, or Podman with
  `HEALER_SANDBOX_TEST_ENGINE=podman`, and the small image of `tests/sandbox/`:

  ```bash
  docker build -t dag-healer/sandbox-test:dev tests/sandbox
  HEALER_SANDBOX_TEST_IMAGE=dag-healer/sandbox-test:dev \
    .venv/bin/python -m pytest tests/unit/test_sandbox_engine.py
  ```

  Without `HEALER_SANDBOX_TEST_IMAGE` these tests are skipped. Run them with
  `HEALER_SANDBOX_TEST_IMAGE=dag-healer/airflow:dev` too when the change touches how the sandbox
  finds Python packages: the test image installs pytest with `pip install --user`, the
  development image keeps its packages in a virtualenv under the `airflow` user's `~/.local`.

The unit tests never need a container engine. The container backend is tested with a fake
engine, and every test that runs a repair selects the `process` backend in its own
configuration (`repo: {sandbox: {backend: process}}`). Keep it that way: never make a test
depend on the default backend (`container`) or on an engine on the developer's machine.

CI (`.github/workflows/ci.yml`) runs on every pull request and every push to `main`: the
ruff checks above, the unit tests on Linux, macOS and Windows (Python 3.10 and 3.13; Linux
also 3.11 and 3.12), the container sandbox tests against Docker on Linux (job "Container
sandbox (Docker)", with the image of `tests/sandbox/`), a build of both UI bundles and the
wheel, and the `ui/` checks. It does not run `test-airflow`, so run it yourself when it
applies.

## Commit messages

dag-healer follows [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/),
which keeps the history readable and lets the changelog and version numbers come from it.

```
<type>(<optional scope>): <description>

[optional body]

[optional footers]
```

- **Description**: imperative mood ("add", not "added" or "adds"), lowercase first word,
  no trailing period, and a first line of at most 72 characters.
- **Body**: what changed and why, wrapped at 72 columns.
- **Footers**: `BREAKING CHANGE: <what breaks and how to migrate>`, `Refs: <issue>`,
  `Co-authored-by: <name> <email>`.

| Type | Use it for |
|---|---|
| `feat` | a new user-facing feature |
| `fix` | a bug fix |
| `docs` | documentation only |
| `refactor` | a code change that neither fixes a bug nor adds a feature |
| `perf` | a performance improvement |
| `test` | adding or fixing tests |
| `build` | packaging, dependencies, the Docker image |
| `ci` | CI configuration |
| `chore` | maintenance that touches neither `src/` nor `tests/` |
| `style` | formatting only |
| `revert` | reverting an earlier commit |

The scope is optional: use the package or area the change lives in, for example `agent`,
`airflow`, `api`, `cli`, `contracts`, `core`, `docker`, `masking`, `orchestrators`, `store`,
`ui` or `worker`.

Mark breaking changes with `!` after the type or scope and a `BREAKING CHANGE:` footer.
Changes to `healer.yaml`, the store schema, `/healer/api/v1` or the plugin entry point
usually are.

```
feat(contracts): flag row counts outside the approved band
fix(masking): hash email addresses inside free-text columns
docs: explain the listener and callback adoption modes
refactor(store)!: key incidents by orchestrator instance

BREAKING CHANGE: run `dag-healer store migrate` before starting the worker.
```

## Pull requests

- One logical change per pull request, with tests.
- Use a Conventional Commit as the pull request title too: when a pull request is
  squash-merged, its title becomes the commit on `main`.
- Update the docs and the `healer.yaml` examples when behaviour or configuration changes.
- Discuss it in an issue first before relaxing a security check of the API
  (authentication, DAG permissions, the CSRF header and origin checks) or a masking
  default.

## Security issues

Do not open a public issue for a vulnerability. Report it privately from the repository's
Security tab ("Report a vulnerability").

## License

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE).
