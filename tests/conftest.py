"""Shared pytest fixtures (keep this file tiny: piece-specific fixtures live next to their tests)."""

from __future__ import annotations

import os
import sys
import textwrap
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path

import pytest

TESTS = Path(__file__).resolve().parent
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
    "HEALER_LLM_MODEL",
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


#: Every AWS setting (and the anthropic SDK's Amazon Bedrock settings) that a test could otherwise
#: inherit from the developer's or CI's environment; :func:`no_machine_aws` unsets them all for
#: every test, and every other variable whose name starts with one of :data:`AWS_ENV_PREFIXES`.
#: The list names the inputs of botocore's credential providers and of the SDK's client, so they
#: are cleared even on a platform that compares names differently: the keys, an expiry
#: (``AWS_CREDENTIAL_EXPIRATION``, which ``aws configure export-credentials --format env`` exports
#: and which, once past, makes every signature fail), the account, profiles, roles, the container
#: and instance metadata endpoints, the endpoint overrides, the retry and signing settings.
AWS_ENV_VARS: tuple[str, ...] = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
    "AWS_CREDENTIAL_EXPIRATION",
    "AWS_ACCOUNT_ID",
    "AWS_ACCESS_KEY",
    "AWS_SECRET_KEY",
    "AWS_PROFILE",
    "AWS_DEFAULT_PROFILE",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "AWS_BEARER_TOKEN_BEDROCK",
    "AWS_ROLE_ARN",
    "AWS_ROLE_SESSION_NAME",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
    "AWS_EC2_METADATA_SERVICE_ENDPOINT",
    "AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE",
    "AWS_EC2_METADATA_V1_DISABLED",
    "AWS_METADATA_SERVICE_TIMEOUT",
    "AWS_METADATA_SERVICE_NUM_ATTEMPTS",
    "AWS_ENDPOINT_URL",
    "AWS_ENDPOINT_URL_BEDROCK_RUNTIME",
    "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS",
    "AWS_USE_FIPS_ENDPOINT",
    "AWS_USE_DUALSTACK_ENDPOINT",
    "AWS_STS_REGIONAL_ENDPOINTS",
    "AWS_MAX_ATTEMPTS",
    "AWS_RETRY_MODE",
    "AWS_AUTH_SCHEME_PREFERENCE",
    "AWS_SIGV4A_SIGNING_REGION_SET",
    "AWS_DATA_PATH",
    "AWS_CA_BUNDLE",
    "AWS_CREDENTIAL_FILE",
    "ANTHROPIC_BEDROCK_BASE_URL",
    "ANTHROPIC_BEDROCK_MANTLE_BASE_URL",
    "ANTHROPIC_AWS_API_KEY",
)
#: Prefixes of the variables :func:`no_machine_aws` also unsets when the environment has them.
AWS_ENV_PREFIXES: tuple[str, ...] = ("AWS_", "BOTO_", "ANTHROPIC_BEDROCK_", "ANTHROPIC_AWS_")
#: dag-healer's own choice of the provider and the model, which a shell set up for Amazon Bedrock
#: exports too (docs/bedrock.md): :func:`no_machine_aws` unsets them for every test, and
#: :func:`isolated_aws` again; a test that needs a provider sets it itself.
LLM_ENV_VARS: tuple[str, ...] = ("HEALER_LLM_PROVIDER", "HEALER_LLM_MODEL")
#: The folders of the opt-in tests that use real services with the machine's own settings (outside
#: the default ``testpaths``, never run in CI): the live Amazon Bedrock test, whose region and
#: credentials come from the usual AWS chain, and the end-to-end test of the Docker stack, whose
#: AWS credentials come from the shell. :func:`no_machine_aws` leaves their tests alone.
MACHINE_SETTINGS_FOLDERS: tuple[Path, ...] = (TESTS / "live", TESTS / "e2e")


def _forget_bedrock_sessions() -> None:
    """The anthropic SDK caches a boto3 session per settings: a test must never sign with another's
    (nothing is cached before the SDK's Bedrock module is imported, so it is not imported here)."""
    auth = sys.modules.get("anthropic.lib.bedrock._auth")
    clear = getattr(getattr(auth, "_get_session", None), "cache_clear", None)
    if callable(clear):
        clear()


def _unset_aws(monkeypatch: pytest.MonkeyPatch, folder: Path, *, also: tuple[str, ...] = ()) -> None:
    """Unset the AWS settings of the environment and point the AWS files into ``folder``.

    The variables of :data:`AWS_ENV_VARS` and ``also``, and any other variable of the environment
    whose name starts with one of :data:`AWS_ENV_PREFIXES`, are unset (set, then deleted, so their
    state is restored at teardown); the shared AWS config and credentials files point at the
    folder's ``aws-config`` and ``aws-credentials`` and boto's config file at its ``boto.cfg``;
    the instance metadata service is disabled; the SDK's cached boto3 sessions are forgotten.
    """
    found = [name for name in os.environ if name.upper().startswith(AWS_ENV_PREFIXES)]
    for name in dict.fromkeys([*AWS_ENV_VARS, *also, *found]):
        monkeypatch.setenv(name, "")
        monkeypatch.delenv(name)
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(folder / "aws-config"))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(folder / "aws-credentials"))
    monkeypatch.setenv("BOTO_CONFIG", str(folder / "boto.cfg"))
    _forget_bedrock_sessions()


def _uses_machine_settings(path: Path) -> bool:
    """True for a test file of :data:`MACHINE_SETTINGS_FOLDERS`."""
    parents = Path(path).resolve().parents
    return any(folder in parents for folder in MACHINE_SETTINGS_FOLDERS)


@pytest.fixture(autouse=True)
def no_machine_aws(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[None]:
    """No AWS setting of the machine reaches a test: the keys, profile or region of a developer's
    shell or of CI (such as AWS's example keys, which moto and LocalStack setups export) never
    change what a test sees, and no test can sign with real credentials.

    For every test, the AWS variables are unset (:data:`AWS_ENV_VARS` and every other variable
    whose name starts with one of :data:`AWS_ENV_PREFIXES`) with the provider and the model of
    :data:`LLM_ENV_VARS`, the shared AWS config and credentials files and boto's config file
    point at paths that do not exist, the instance metadata service is disabled and the
    anthropic SDK's cached boto3 sessions are forgotten, before and after. A test that needs an
    AWS value or a provider sets it itself (``monkeypatch.setenv``); one that needs AWS files of
    its own requests :func:`isolated_aws`. The tests of :data:`MACHINE_SETTINGS_FOLDERS` keep the
    machine's settings.
    """
    if _uses_machine_settings(request.path):
        yield
        return
    _unset_aws(monkeypatch, tmp_path_factory.getbasetemp() / "no-aws-files", also=LLM_ENV_VARS)
    yield
    _forget_bedrock_sessions()


@pytest.fixture
def isolated_aws(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[Path]:
    """A folder for the AWS files of a test, in the isolation of :func:`no_machine_aws`.

    The AWS settings are unset again, whatever a fixture requested before this one set, with
    :data:`LLM_ENV_VARS`; the shared AWS config and credentials files point at the folder's
    ``aws-config`` and ``aws-credentials`` and boto's config file at its ``boto.cfg``, all absent
    until a test writes them (``encoding="utf-8", newline="\\n"``); the instance metadata
    service stays disabled; the SDK's cached boto3 sessions are forgotten before and after.
    """
    folder = tmp_path / "aws"
    folder.mkdir()
    _unset_aws(monkeypatch, folder, also=LLM_ENV_VARS)
    yield folder
    _forget_bedrock_sessions()
