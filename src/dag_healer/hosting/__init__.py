"""The standalone host of dag-healer (``dag-healer serve``).

It serves the API and the React views of the Airflow plugin without Airflow, for orchestrators
that have no UI plugin system (section 7 of ``docs/architecture/orchestrators.md``):

* :mod:`dag_healer.hosting.app`: :func:`~dag_healer.hosting.app.create_standalone_app`, the
  ASGI app of ``dag-healer serve`` (the healer app under ``/healer`` plus the shell);
* :mod:`dag_healer.hosting.shell`: the SPA shell (``index.html``) that loads the standalone UI
  bundle on the URL shapes of the Airflow host;
* :mod:`dag_healer.hosting.rbac`: :class:`~dag_healer.hosting.rbac.RuleAuthorizer`, the
  permissions of ``serve.auth.rules``;
* :mod:`dag_healer.hosting.auth`: the :class:`~dag_healer.api.auth.AuthBackend`
  implementations (``dev``, ``token``, ``trusted_header``, ``alb_oidc``).

Dependency direction: ``hosting`` imports ``api``, ``config``, ``masking`` and
``orchestrators.registry``; only ``cli/serve_cmd.py`` imports ``hosting`` (nothing in ``api/``,
the core, the adapters or the task runners does). It is neutral like the core: it never imports
Airflow, an AWS SDK, an adapter package, the fake adapter or the conformance suite
(``tests/unit/test_architecture.py``). Nothing is imported here, so that importing
``dag_healer.hosting.<module>`` stays cheap.
"""
