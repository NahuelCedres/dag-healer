# Configuration fixtures

Used by `tests/unit/test_config_orchestrator.py` (step 3 of section 12.1 of
`docs/architecture/orchestrators.md`).

## `phase5/`: frozen phase-5 configurations

Copies of the `healer.yaml` files dag-healer produced or used before the orchestrator
refactor. They must keep loading to the same effective settings:

| File | Origin |
|---|---|
| `default_template.yaml` | `default_config_yaml()` of phase 5 |
| `init_example_repo.yaml` | the `healer.yaml` that `dag-healer init` wrote for `example_existing_repo` |
| `demo.yaml` | `.demo/repo/healer.yaml` of the demo stack (`scripts/demo.py`) |
| `demo_listener_only.yaml` | the demo settings with `listener_only=True` |
| `api_routes.yaml`, `api_security.yaml`, `airflow_container.yaml`, `cli_check_secrets.yaml` | the `healer.yaml` texts written by `test_api_routes.py`, `test_api_security.py`, `tests/airflow/conftest.py` and `test_cli_commands.py` |
| `custom_all_sections.yaml` | hand-written: every section set to a non-default value |
| `contracts/customers_sync.yaml` | the approved demo contract, with the `source_hash` phase 5 computed |

`effective_settings.json` was recorded with the phase-5 `config.py`. For every file, under an
empty and under a filled environment, it holds the `dag-healer check` YAML (paths normalised to
`<CONFIG_DIR>` and `<HEALER_HOME>`) and `secret_literals()`.

Re-record only after an intended change of the effective settings, and review the diff in the
same change:

```
HEALER_UPDATE_GOLDEN=1 python -m pytest tests/unit/test_config_orchestrator.py
```
