"""``dag-healer serve [--host HOST] [--port PORT]``: the Healer API and UI without Airflow.

Runs the standalone host (section 7 of ``docs/architecture/orchestrators.md``) with uvicorn:
the API of the Airflow plugin under ``/healer`` (``/healer/api/v1``, the public
``/healer/health``, ``/healer/static``) and the Healer views on the URL shapes of the Airflow
host (``/plugin/healer``, ``/dags/<d>/plugin/healer-dag``,
``/dags/<d>/runs/<r>/tasks/<t>/plugin/healer-ti``), for orchestrators without a UI plugin
system. The address is ``serve.host`` / ``serve.port`` of ``healer.yaml`` unless ``--host`` /
``--port`` override it; ``localhost`` binds ``127.0.0.1``.

Users authenticate with ``serve.auth.backend``: ``dev`` (a fixed local user, refused unless the
address is loopback), ``token`` (bearer tokens listed by their sha256), ``trusted_header`` (a
user header set by a proxy listed in ``trusted_proxies``) or ``alb_oidc`` (the JWT an AWS ALB
signs); ``serve.auth.rules`` say who may read, act on or approve the contract of which
pipeline. Both are read once, at start: a token removed from ``healer.yaml`` keeps working until
the server restarts (the host warns when it sees the change). uvicorn runs without proxy
headers: ``X-Forwarded-For`` and ``X-Forwarded-Proto`` are ignored, so the client address the
``trusted_header`` backend checks is the direct TCP peer.

The configuration file is exported as ``HEALER_CONFIG``, because the API reloads ``healer.yaml``
on its own. The store is created or migrated at start; a store that cannot be reached is a
warning (its routes answer 503 until it can be). Exit status: 1 when the host cannot start (an
invalid configuration, an authentication backend that cannot be served safely, an orchestrator
adapter that cannot be built, uvicorn missing: ``pip install 'dag-healer[serve]'``); 0 once the
server stops. Heavy imports stay inside the handler so ``--help`` is fast.
"""

from __future__ import annotations

import argparse
from typing import Any

from dag_healer.cli._output import add_config_option, error, load_cli_config, mask_url, warn
from dag_healer.cli.main import register_command

SERVE_EXTRA_HINT = "pip install 'dag-healer[serve]'"
UVICORN_MISSING = f"dag-healer serve needs uvicorn: {SERVE_EXTRA_HINT}"
#: The address bound for the host name ``localhost``.
LOOPBACK_ADDRESS = "127.0.0.1"
_MAX_PORT = 65535


def _bind_address(value: str) -> str:
    """``value`` stripped, without the brackets of an IPv6 literal (``[::1]`` -> ``::1``).

    ``localhost`` (any case) is ``127.0.0.1``: uvicorn would bind what the name resolves to, and
    only the literal address is sure to be loopback (the ``dev`` backend depends on it).
    """
    host = value.strip()
    host = host[1:-1] if host.startswith("[") and host.endswith("]") else host
    return LOOPBACK_ADDRESS if host.lower() == "localhost" else host


def _host(value: str) -> str:
    host = _bind_address(value)
    if not host:
        raise argparse.ArgumentTypeError("expected a host name or an IP address")
    return host


def _port(value: str) -> int:
    try:
        port = int(value)
    except ValueError:
        port = 0
    if not 1 <= port <= _MAX_PORT:
        raise argparse.ArgumentTypeError(f"invalid port {value!r}: expected an integer from 1 to {_MAX_PORT}")
    return port


def _configure(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--host",
        type=_host,
        default=None,
        help="Address to bind (default: serve.host of healer.yaml, 127.0.0.1).",
    )
    parser.add_argument(
        "--port", type=_port, default=None, help="Port to bind (default: serve.port of healer.yaml, 8090)."
    )
    add_config_option(parser)


def run_server(app: Any, *, host: str, port: int, log_level: str = "info") -> None:
    """Serve ``app`` with uvicorn until it stops.

    ``proxy_headers=False``: ``request.client`` stays the TCP peer (which the ``trusted_header``
    backend checks) and ``X-Forwarded-For`` / ``X-Forwarded-Proto`` are ignored.
    """
    import uvicorn

    uvicorn.run(app, host=host, port=port, proxy_headers=False, server_header=False, log_level=log_level)


def _uvicorn_installed() -> bool:
    try:
        import uvicorn  # noqa: F401
    except ImportError:
        return False
    return True


def _error_text(exc: BaseException) -> str:
    """``Type: first line of the message``, bounded (never a traceback)."""
    message = str(exc).strip().splitlines()[0] if str(exc).strip() else ""
    return (f"{type(exc).__name__}: {message}" if message else type(exc).__name__)[:400]


def _url(host: str, port: int) -> str:
    return f"http://[{host}]:{port}" if ":" in host else f"http://{host}:{port}"


def _prepare_store(cfg: Any) -> None:
    """Create or migrate the store (section 6.4); a store that cannot be reached is only a warning."""
    from dag_healer.store.db import get_engine, init_db

    url = cfg.database_url
    try:
        init_db(get_engine(url), allow_migrate=True)
    except Exception as exc:  # driver-specific exception types
        warn(
            f"the store {mask_url(url)} cannot be prepared ({_error_text(exc)}); its routes answer 503 "
            "until it can be"
        )


def _startup_warnings(cfg: Any, host: str) -> list[str]:
    """What the operator should know before the host serves (the dev backend, a proxy setup).

    The groups header of ``trusted_header`` is believed from the trusted proxies like the user
    header, so a proxy that only sets the user header lets clients name their own groups. The
    empty-rules, missing-bundle and UI-host warnings are logged while the app is built.
    """
    from dag_healer.hosting.auth import standalone_origins
    from dag_healer.hosting.auth.dev import is_loopback_host

    messages: list[str] = []
    auth = cfg.serve.auth
    if auth.backend == "dev":
        messages.append(
            "the dev backend serves every request as the user 'dev' (group admin), without "
            "authentication: use it for local development only"
        )
    if auth.backend == "trusted_header" and auth.groups_header is not None:
        default = "" if "groups_header" in auth.model_fields_set else " (the default)"
        messages.append(
            f"serve.auth trusts the {auth.groups_header} header{default} from the trusted proxies: they "
            "must set or strip it on every request, or a client can claim any group; set "
            "serve.auth.groups_header: null if they send no groups"
        )
    if not standalone_origins(cfg) and not is_loopback_host(host):
        messages.append(
            f"{host} is not a loopback address and neither serve.public_url nor HEALER_PUBLIC_URL is "
            "set: behind a proxy, set serve.public_url to the URL users open, or the browser's "
            "state-changing requests fail the CSRF origin check and links point at the bind address"
        )
    return messages


def _run(args: argparse.Namespace) -> int:
    import os

    from dag_healer.config import CONFIG_ENV, ConfigError

    if not _uvicorn_installed():
        error(UVICORN_MISSING)
        return 1
    try:
        cfg = load_cli_config(args)
    except ConfigError as exc:
        error(str(exc))
        return 1
    host = args.host or _bind_address(cfg.serve.host)
    port = args.port if args.port is not None else cfg.serve.port
    if cfg.source_path is not None:
        os.environ[CONFIG_ENV] = str(cfg.source_path)

    from dag_healer.orchestrators import registry

    try:
        adapter = registry.get_adapter(cfg)
    except registry.AdapterLoadError as exc:
        error(f"the orchestrator adapter cannot be loaded: {exc}")
        return 1
    except Exception as exc:  # a factory that fails in its own way
        from dag_healer.masking import mask_text

        error(f"the orchestrator adapter cannot be built: {mask_text(_error_text(exc))}")
        return 1

    from dag_healer.hosting.app import create_standalone_app

    try:
        app = create_standalone_app(cfg, bind_host=host)
    except (ConfigError, registry.AdapterLoadError) as exc:  # AuthConfigError is a ConfigError
        error(str(exc))
        return 1
    _prepare_store(cfg)
    for message in _startup_warnings(cfg, host):
        warn(message)
    print(
        f"dag-healer serve: {_url(host, port)}/plugin/healer (auth: {cfg.serve.auth.backend}, "
        f"orchestrator: {adapter.display_name})",
        flush=True,
    )
    run_server(app, host=host, port=port, log_level="debug" if getattr(args, "verbose", 0) >= 2 else "info")
    return 0


register_command(
    "serve",
    help="Serve the Healer API and UI without Airflow (standalone host).",
    handler=_run,
    configure=_configure,
)

__all__ = ["run_server"]
