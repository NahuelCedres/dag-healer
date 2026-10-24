"""The API route suites on every host (section 11.7 of ``docs/architecture/orchestrators.md``).

``test_api_routes.py`` and ``test_api_security.py`` run the API with Airflow's user and
authorizer faked by dependency overrides. This module collects a representative set of their
tests again, by name (below), and runs each of them on every host:

* ``airflow``: those faked Airflow overrides, exactly as there;
* ``token``: ``create_app(auth=TokenAuthBackend)``, the app the standalone host mounts under
  ``/healer``, with a :class:`~dag_healer.hosting.rbac.RuleAuthorizer` whose rules give the
  users the permissions of the faked Airflow users;
* ``alb_oidc``: the same with ``AlbOidcAuthBackend``: ALB-style (padded) ES256 tokens signed by
  a local key, the key endpoint stubbed;
* ``trusted_header``: the same with ``TrustedHeaderAuthBackend``, the client being a trusted
  proxy.

The tests name their user with the ``X-Test-User`` header; the client of each host replaces it
with that host's credentials for the user (:class:`Host`). The tests left out depend on the
faked Airflow users themselves (an invalid JWT answers 403 there, every bad credential answers
401 on the standalone host) or on the exact ``/me`` body (the standalone host adds ``host``);
the tests at the end cover those cases on each host, and the credentials each standalone
backend refuses. Mounting under ``/healer`` and the shell are covered by
``test_standalone_host.py``.
"""

from __future__ import annotations

import functools
import re
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import jwt
import pytest
import test_api_routes as routes  # tests/unit is on sys.path (pytest's default prepend import mode)
import test_api_security as security
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from dag_healer.airflow.adapter import AirflowAdapter
from dag_healer.api import auth, deps, views
from dag_healer.api.app import create_app
from dag_healer.config import HealerConfig, config_from_mapping, load_config
from dag_healer.contracts.io import save_contract
from dag_healer.contracts.schema import ColumnRule, Contract, DatasetContract, DatasetLocator
from dag_healer.core.vocab import Vocabulary
from dag_healer.hosting.app import pipeline_candidates
from dag_healer.hosting.auth import standalone_origins
from dag_healer.hosting.auth.alb_oidc import AlbOidcAuthBackend, SigningKeyNotFound, alb_key_url
from dag_healer.hosting.auth.token import TokenAuthBackend, token_sha256
from dag_healer.hosting.auth.trusted_header import TrustedHeaderAuthBackend
from dag_healer.hosting.rbac import RuleAuthorizer
from dag_healer.orchestrators.fake import FAKE_CAPABILITIES, FAKE_VOCABULARY, FakeAdapter
from dag_healer.store import db
from dag_healer.store import repo as store_repo

HOSTS = ("airflow", "token", "alb_oidc", "trusted_header")
NOT_AUTHENTICATED = {"detail": "Not authenticated"}

#: The groups of the users the two suites name.
GROUPS: dict[str, tuple[str, ...]] = {
    "admin": ("admins",),
    "viewer": ("viewers",),
    "limited": (),
    "outsider": (),
}
EVERY_ACTION = ["read", "act", "approve_contract"]
#: The permissions of the faked Airflow users of both suites, as ``serve.auth.rules``.
RULES: list[dict[str, Any]] = [
    {"principals": ["group:admins"], "pipelines": ["*"], "actions": EVERY_ACTION},
    {"principals": ["group:viewers"], "pipelines": ["*"], "actions": ["read"]},
    {"principals": ["user:limited"], "pipelines": ["sales_daily_etl", "customers_sync"], "actions": ["read"]},
    {"principals": ["user:limited"], "pipelines": ["customers_sync"], "actions": ["act", "approve_contract"]},
    {"principals": ["user:outsider"], "pipelines": ["other_dag"], "actions": EVERY_ACTION},
]

ALB_ARN = "arn:aws:elasticloadbalancing:eu-west-1:123456789012:loadbalancer/app/healer/50dc6c495c0c9188"
OTHER_ALB_ARN = "arn:aws:elasticloadbalancing:eu-west-1:210987654321:loadbalancer/app/other/0123456789abcdef"
ALB_REGION = "eu-west-1"
ALB_KID = "3f2b8c1d-5e6a-4b7c-8d9e-0f1a2b3c4d5e"
ISSUER = "https://idp.example.com"
#: The proxy the trusted_header host trusts, and an address it does not.
TRUSTED_PROXIES = ["10.0.0.0/24"]
PROXY_PEER = ("10.0.0.5", 50000)
UNTRUSTED_PEER = ("192.0.2.10", 1234)
#: The UI's words when ``/me`` has no orchestrator block.
UI_VOCABULARY = Path(__file__).resolve().parents[2] / "ui" / "src" / "vocabulary.ts"


# --------------------------------------------------------------------------- hosts


class RecordingRuleAuthorizer(RuleAuthorizer):
    """The standalone host's authorizer for :data:`RULES`, recording its checks like ``FakeAuthorizer``."""

    def __init__(self) -> None:
        super().__init__(RULES, candidates=pipeline_candidates)
        self.checks: list[tuple[str, str | None, str | None]] = []

    async def check_dag_access(self, request: Request, user: Any, method: str, entity: str | None) -> None:
        self.checks.append((method, entity, request.path_params.get("dag_id")))
        await super().check_dag_access(request, user, method, entity)


@functools.cache
def alb_key() -> ec.EllipticCurvePrivateKey:
    """The load balancer's signing key."""
    return ec.generate_private_key(ec.SECP256R1())


def alb_token(
    user: str,
    *,
    groups: tuple[str, ...] = (),
    key: ec.EllipticCurvePrivateKey | None = None,
    signer: str = ALB_ARN,
    header_lifetime: int = 300,
) -> str:
    """An ``x-amzn-oidc-data`` JWT as the load balancer sends it: base64 padding on every segment.

    The user is its email, which the identity provider verified (Cognito sends the flag as a string).
    """
    now = int(time.time())
    header = {"kid": ALB_KID, "signer": signer, "iss": ISSUER, "exp": now + header_lifetime}
    claims = {
        "sub": f"sub-{user}",
        "email": user,
        "email_verified": "true",
        "groups": list(groups),
        "iss": ISSUER,
        "exp": now + 300,
    }
    token = jwt.encode(claims, key or alb_key(), algorithm="ES256", headers=header)
    return ".".join(segment + "=" * (-len(segment) % 4) for segment in token.split("."))


class KeyEndpoint:
    """The load balancer's key endpoint (``key_fetcher``): the URLs it was asked for."""

    def __init__(self) -> None:
        self.urls: list[str] = []

    def __call__(self, url: str) -> str:
        self.urls.append(url)
        if url != alb_key_url(ALB_REGION, ALB_KID):
            raise SigningKeyNotFound("HTTP 404")
        public = alb_key().public_key()
        pem = public.public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        return pem.decode("ascii")


def _bearer(user: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {user}-token"}


def _signed(user: str) -> dict[str, str]:
    return {"x-amzn-oidc-data": alb_token(user, groups=GROUPS.get(user, ()))}


def _forwarded(user: str) -> dict[str, str]:
    headers = {"X-Forwarded-User": user}
    if GROUPS.get(user):
        headers["X-Forwarded-Groups"] = ",".join(GROUPS[user])
    return headers


@dataclass
class Host:
    """One host of the suites: its healer app, its authorizer and the credentials of a test user."""

    name: str
    app: FastAPI
    authorizer: Any
    credentials: Callable[[str], dict[str, str]]  # user name -> request headers
    peer: tuple[str, int] = ("testclient", 50000)
    keys: KeyEndpoint | None = None

    def client(self) -> TestClient:
        """A client sending, instead of ``X-Test-User: <name>``, this host's credentials for ``name``."""
        client = TestClient(self.app, client=self.peer)
        client.event_hooks = {"request": [self._sign_in], "response": []}
        return client

    def _sign_in(self, request: Any) -> None:
        user = request.headers.pop("x-test-user", None)
        if user is not None:
            request.headers.update(self.credentials(user))


def build_host(name: str, cfg: HealerConfig, *, airflow: tuple[Callable[..., Any], Any]) -> Host:
    """The host ``name`` for ``cfg``; ``airflow`` is the faked Airflow user dependency and authorizer."""
    if name == "airflow":
        user_dependency, fake_authorizer = airflow
        app = create_app()
        app.dependency_overrides[auth.get_current_user] = user_dependency
        app.dependency_overrides[auth.get_authorizer] = lambda: fake_authorizer
        return Host(name, app, fake_authorizer, lambda user: {"X-Test-User": user})
    authorizer = RecordingRuleAuthorizer()
    origins = standalone_origins(cfg)
    if name == "token":
        tokens = [
            {"name": user, "sha256": token_sha256(f"{user}-token"), "groups": list(groups)}
            for user, groups in GROUPS.items()
        ]
        backend = TokenAuthBackend(tokens, authorizer=authorizer, origins=origins)
        return Host(name, create_app(auth=backend), authorizer, _bearer)
    if name == "alb_oidc":
        keys = KeyEndpoint()
        alb = AlbOidcAuthBackend(
            alb_arn=ALB_ARN,
            region=ALB_REGION,
            groups_claim="groups",
            authorizer=authorizer,
            origins=origins,
            key_fetcher=keys,
        )
        return Host(name, create_app(auth=alb), authorizer, _signed, keys=keys)
    assert name == "trusted_header", name
    proxy = TrustedHeaderAuthBackend(trusted_proxies=TRUSTED_PROXIES, authorizer=authorizer, origins=origins)
    return Host(name, create_app(auth=proxy), authorizer, _forwarded, peer=PROXY_PEER)


def _isolate(
    monkeypatch: pytest.MonkeyPatch, host: str, *, config: Path, db_url: str, ingest_token: str, base_url: str
) -> None:
    """The environment of the suites; ``base_url`` is Airflow's on the plugin, the public URL else."""
    monkeypatch.setenv("HEALER_CONFIG", str(config))
    monkeypatch.setenv("HEALER_DATABASE_URL", db_url)
    monkeypatch.setenv("HEALER_INGEST_TOKEN", ingest_token)
    monkeypatch.setenv("AIRFLOW__API__BASE_URL" if host == "airflow" else "HEALER_PUBLIC_URL", base_url)
    _reset()


def _reset() -> None:
    db.dispose_engines()
    deps.reset_config_cache()
    deps.ingest_rate_limiter.reset()
    deps.channel_rate_limiter.reset()


# --------------------------------------------------------------------------- fixtures


@dataclass
class HostApi(routes.Api):
    """``test_api_routes.Api`` on one host."""

    host: Host


@dataclass
class HostEnv(security.Env):
    """``test_api_security.Env`` on one host."""

    host: Host


@pytest.fixture(params=HOSTS)
def api(
    request: pytest.FixtureRequest, healer_env: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[HostApi]:
    """The repository, store and users of ``test_api_routes``, served by the host ``request.param``."""
    repo = tmp_path / "repo"
    config = routes.write_repo(repo)
    db_url = f"sqlite:///{(tmp_path / 'store' / 'healer.db').as_posix()}"
    _isolate(
        monkeypatch,
        request.param,
        config=config,
        db_url=db_url,
        ingest_token=routes.INGEST_TOKEN,
        base_url=routes.BASE_URL,
    )
    ids = routes.seed(db_url, healer_env)
    fakes = (routes.fake_current_user, routes.FakeAuthorizer())
    host = build_host(request.param, load_config(config), airflow=fakes)
    with host.client() as client:
        yield HostApi(client, repo, healer_env, db_url, host.authorizer, ids, host=host)
    _reset()


@pytest.fixture
def fakeflow(api: HostApi) -> routes.Fakeflow:
    """``test_api_routes.fakeflow`` on this module's hosts: the API serves a Fakeflow instance."""
    adapter = FakeAdapter(world=routes.fakeflow_world())
    api.client.app.dependency_overrides[deps.get_adapter] = lambda: adapter
    return routes.Fakeflow(adapter, routes.seed_fakeflow(api.db_url))


def write_security_repo(root: Path) -> Path:
    """The repository of ``test_api_security``: one contract (inventory_report) and its healer.yaml."""
    (root / "dags").mkdir(parents=True)
    contract = Contract(
        dag_id="inventory_report",
        datasets=[
            DatasetContract(
                name="inventory",
                task_id="build_report",
                locator=DatasetLocator(type="csv", path="data/incoming/inventory.csv"),
                columns=[ColumnRule(name="qty", dtype="integer")],
            )
        ],
    )
    save_contract(contract, root / "healer" / "contracts" / "inventory_report.yaml")
    config = root / "healer.yaml"
    config.write_text("version: 1\nrepo: {path: .}\ncontracts: {dir: healer/contracts}\n", encoding="utf-8")
    return config


@pytest.fixture(params=HOSTS)
def env(
    request: pytest.FixtureRequest, healer_env: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[HostEnv]:
    """The repository, store and users of ``test_api_security``, served by the host ``request.param``."""
    config = write_security_repo(tmp_path / "repo")
    db_url = f"sqlite:///{(tmp_path / 'healer.db').as_posix()}"
    _isolate(
        monkeypatch,
        request.param,
        config=config,
        db_url=db_url,
        ingest_token=security.TOKEN,
        base_url=security.BASE_URL,
    )
    with db.session_scope(db_url) as session:
        incident, _ = store_repo.create_incident(
            session,
            dag_id="inventory_report",
            run_id="manual__1",
            task_id="build_report",
            try_number=1,
            source="rest",
            status="waiting_human",
        )
        incident_id = incident.id
    fakes = (security.fake_current_user, security.FakeAuthorizer())
    host = build_host(request.param, load_config(config), airflow=fakes)
    with host.client() as client:
        yield HostEnv(client, host.app, host.authorizer, incident_id, host=host)
    _reset()


# --------------------------------------------------------------------------- the route suite on every host

# reads and filters
test_list_incidents_admin = routes.test_list_incidents_admin
test_list_incidents_filters = routes.test_list_incidents_filters
test_list_incidents_only_readable_dags = routes.test_list_incidents_only_readable_dags
test_dag_incidents = routes.test_dag_incidents
test_incident_detail = routes.test_incident_detail
test_incident_detail_never_leaks_raw_values = routes.test_incident_detail_never_leaks_raw_values
test_incident_detail_actions_by_permission_and_status = (
    routes.test_incident_detail_actions_by_permission_and_status
)
test_incident_detail_not_found = routes.test_incident_detail_not_found
test_pr_md = routes.test_pr_md
test_pr_md_outside_healer_home_is_not_served = routes.test_pr_md_outside_healer_home_is_not_served
test_task_instance_incident = routes.test_task_instance_incident
test_dags_list = routes.test_dags_list
test_contracts_list_and_detail = routes.test_contracts_list_and_detail
test_contract_reload_picks_up_file_edits = routes.test_contract_reload_picks_up_file_edits
# actions and the contract approval
test_resolve_enqueues_command = routes.test_resolve_enqueues_command
test_resolve_validates_status_and_body = routes.test_resolve_validates_status_and_body
test_retry_repair_and_retry_task = routes.test_retry_repair_and_retry_task
test_approve_contract = routes.test_approve_contract
test_approve_carries_hash_of_reviewed_file = routes.test_approve_carries_hash_of_reviewed_file
# events (the ingest token, whatever the host)
test_event_is_stored_masked = routes.test_event_is_stored_masked
test_event_token_checks = routes.test_event_token_checks
test_channel_event_is_stored_masked = routes.test_channel_event_is_stored_masked
# the store and the orchestrator instance
test_store_errors_answer_503 = routes.test_store_errors_answer_503
test_an_outdated_store_answers_503_and_is_never_migrated = (
    routes.test_an_outdated_store_answers_503_and_is_never_migrated
)
test_default_responses_leave_out_the_additive_fields = (
    routes.test_default_responses_leave_out_the_additive_fields
)
test_incidents_of_another_instance_are_neither_listed_nor_shown = (
    routes.test_incidents_of_another_instance_are_neither_listed_nor_shown
)
test_commands_and_events_carry_the_instance = routes.test_commands_and_events_carry_the_instance
# a Fakeflow instance: additive fields, its worker's commands, the capabilities of its adapter
test_fakeflow_lists_and_details_carry_the_additive_fields = (
    routes.test_fakeflow_lists_and_details_carry_the_additive_fields
)
test_fakeflow_commands_are_queued_for_its_worker = routes.test_fakeflow_commands_are_queued_for_its_worker
test_retry_task_needs_a_remediator_that_offers_it = routes.test_retry_task_needs_a_remediator_that_offers_it
test_ci_deploy_warns_before_retrying_a_repaired_incident = (
    routes.test_ci_deploy_warns_before_retrying_a_repaired_incident
)

# --------------------------------------------------------------------------- the security suite on every host

test_unauthenticated_requests_get_401 = security.test_unauthenticated_requests_get_401
test_user_without_dag_access_gets_403 = security.test_user_without_dag_access_gets_403
test_viewer_cannot_act = security.test_viewer_cannot_act
test_post_requires_requested_with_header = security.test_post_requires_requested_with_header
test_post_origin_checks = security.test_post_origin_checks
test_events_are_exempt_from_csrf_but_need_the_token = (
    security.test_events_are_exempt_from_csrf_but_need_the_token
)
test_channel_events_are_exempt_from_csrf_but_need_the_token = (
    security.test_channel_events_are_exempt_from_csrf_but_need_the_token
)
test_request_body_is_capped = security.test_request_body_is_capped
test_get_requests_are_not_csrf_checked = security.test_get_requests_are_not_csrf_checked


# --------------------------------------------------------------------------- /me and the capabilities


def test_me_names_the_host(api: HostApi) -> None:
    expected = {"user": "viewer", "base_url": routes.BASE_URL}
    if api.host.name != "airflow":
        expected["host"] = "standalone"
    assert api.get("/me", user="viewer").json() == expected


def test_me_and_the_actions_follow_the_adapter(api: HostApi, fakeflow: routes.Fakeflow) -> None:
    me = api.get("/me", user="viewer").json()
    assert ("host" in me) is (api.host.name != "airflow")
    orchestrator = me["orchestrator"]
    assert orchestrator["display_name"] == "Fakeflow"
    assert orchestrator["vocabulary"] == FAKE_VOCABULARY.as_dict()
    remediations = orchestrator["capabilities"]["remediations"]
    assert remediations == [kind.value for kind in FAKE_CAPABILITIES.remediations]
    path = f"/dags/sales_daily_etl/incidents/{fakeflow.ids['reported']}"
    assert api.get(path).json()["allowed_actions"] == ["resolve_code", "retry_task", "ignore"]

    # an adapter without a remediator: /me lists no remediation and nothing offers retry_task
    fakeflow.adapter.capabilities = replace(FAKE_CAPABILITIES, remediations=())
    fakeflow.adapter.remediator = None
    assert api.get("/me", user="viewer").json()["orchestrator"]["capabilities"]["remediations"] == []
    assert api.get(path).json()["allowed_actions"] == ["resolve_code", "ignore"]
    assert api.post(f"{path}/retry-task").status_code == 409


def test_the_ui_defaults_say_what_me_leaves_out_for_airflow() -> None:
    """``/me`` has no orchestrator block for the default Airflow instance, and the plugin never asks.

    The UI's defaults (``ui/src/vocabulary.ts``) then stand for it, so they must say what the
    block says for a named Airflow instance.
    """
    text = UI_VOCABULARY.read_text(encoding="utf-8")
    words = re.search(r"export const DEFAULT_VOCABULARY: Vocabulary = \{(?P<body>[^}]*)\};", text)
    display_name = re.search(r'export const DEFAULT_DISPLAY_NAME = "(?P<name>[^"]*)";', text)
    assert words is not None and display_name is not None
    ui_words = dict(re.findall(r'(\w+): "([^"]*)"', words.group("body")))
    prod = config_from_mapping({"orchestrator": {"instance": "prod"}})
    named = views.orchestrator_view(AirflowAdapter(prod))
    assert named is not None
    assert ui_words == named.vocabulary == Vocabulary().as_dict()
    assert display_name.group("name") == named.display_name == "Apache Airflow"


# --------------------------------------------------------------------------- standalone credentials


@pytest.mark.parametrize("env", ["token", "alb_oidc"], indirect=True)
def test_credentials_the_host_does_not_know_get_401(env: HostEnv) -> None:
    """Airflow answers an invalid JWT with 403; the standalone backends answer 401 to any bad credential."""
    if env.host.name == "token":
        forged = {"Authorization": "Bearer forged-token"}
    else:  # the admin's token, signed by another key than the load balancer's
        other_key = ec.generate_private_key(ec.SECP256R1())
        forged = {"x-amzn-oidc-data": alb_token("admin", groups=("admins",), key=other_key)}
    for method, path, body in security._route_samples(env.incident_id):
        response = security._call(env, method, path, body, forged)
        assert response.status_code == 401, (method, path)
        assert response.json() == NOT_AUTHENTICATED
        if env.host.name == "token":
            assert response.headers["www-authenticate"] == "Bearer"


@pytest.mark.parametrize("env", ["alb_oidc"], indirect=True)
@pytest.mark.parametrize(
    "token_options",
    [{"signer": OTHER_ALB_ARN}, {"header_lifetime": -60}],
    ids=["wrong-signer", "expired-header-exp"],
)
def test_alb_tokens_of_another_signer_or_past_their_header_exp_get_401(
    env: HostEnv, token_options: dict[str, Any]
) -> None:
    for method, path, body in security._route_samples(env.incident_id):
        token = alb_token("admin", groups=("admins",), **token_options)
        response = security._call(env, method, path, body, {"x-amzn-oidc-data": token})
        assert response.status_code == 401, (method, path)
        assert response.json() == NOT_AUTHENTICATED
    assert env.host.keys is not None and env.host.keys.urls == []  # refused before any key fetch


@pytest.mark.parametrize("env", ["alb_oidc"], indirect=True)
def test_alb_tokens_are_padded_and_the_key_is_fetched_once(env: HostEnv) -> None:
    token = alb_token("viewer", groups=("viewers",))
    assert token.endswith("==")  # the base64 padding of the ES256 signature, as the ALB sends it
    for _ in range(3):
        me = env.client.get("/api/v1/me", headers={"x-amzn-oidc-data": token})
        assert me.status_code == 200 and me.json()["user"] == "viewer"
    assert env.host.keys is not None and env.host.keys.urls == [alb_key_url(ALB_REGION, ALB_KID)]


@pytest.mark.parametrize("env", ["alb_oidc"], indirect=True)
def test_the_unsigned_alb_headers_alone_get_401(env: HostEnv) -> None:
    unsigned = {"x-amzn-oidc-identity": "admin", "x-amzn-oidc-accesstoken": "access-token"}
    for method, path, body in security._route_samples(env.incident_id):
        response = security._call(env, method, path, body, unsigned)
        assert response.status_code == 401, (method, path)
        assert response.json() == NOT_AUTHENTICATED


@pytest.mark.parametrize("env", ["trusted_header"], indirect=True)
def test_identity_headers_count_only_from_a_trusted_proxy(env: HostEnv) -> None:
    admin = _forwarded("admin")
    # X-Forwarded-For naming the trusted proxy changes nothing: the peer is the socket address
    forwarded = {**admin, "X-Forwarded-For": PROXY_PEER[0]}
    with TestClient(env.app, client=UNTRUSTED_PEER) as untrusted:
        for method, path, body in security._route_samples(env.incident_id):
            response = security._call(replace(env, client=untrusted), method, path, body, forwarded)
            assert response.status_code == 401, (method, path)
            assert response.json() == NOT_AUTHENTICATED
    pages = ("/api/v1/me", "/api/v1/incidents", f"/api/v1/dags/inventory_report/incidents/{env.incident_id}")
    for path in pages:
        assert env.client.get(path, headers=admin).status_code == 200, path
