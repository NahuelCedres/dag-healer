# Golden regression snapshots (phase 5 baseline)

Recorded by `tests/unit/test_golden_snapshots.py` (section 11.5 of
`docs/architecture/orchestrators.md`). The test replays the three demo incidents
deterministically and compares every file here byte for byte:

| Path | What |
|---|---|
| `system/classify_system.txt` | system prompt of the classification (the same for every incident) |
| `system/repair_system.txt` | system prompt of the repair sessions |
| `<case>/classify_prompt.txt` | user prompt of the classification |
| `customers/repair_<n>_<phase>_prompt.txt` | user prompt of each repair session (`test`, then `fix`) |
| `customers/repair_<n>_<phase>_tools.txt` | name, signature and docstring of the tools of that session |
| `customers/PR.md` | the PR description written for the CODE incident |
| `<case>/alert_<event>.txt` / `.html` | the alert email of the final state (subject + text body, HTML body) |
| `<case>/incident_detail.json` | `GET /healer/api/v1/dags/{dag_id}/incidents/{id}` |
| `config/default_config.yaml` | `dag_healer.config.default_config_yaml()` |
| `config/init_healer.yaml` | the `healer.yaml` written by `dag-healer init` for `example_existing_repo` |
| `config/check_output.txt` | `dag-healer check` for that `healer.yaml` |

Cases: `sales` (DATA, `reported`), `customers` (CODE, `repaired`), `inventory`
(AMBIGUOUS, `waiting_human`).

Normalised values (everything else is literal): temporary paths (`<REPO>`, `<HEALER_HOME>`),
wall-clock times (`<timestamp>`, `<generated_at>`), the package version (`<version>`) and
the pytest output inside the repair (reduced to its `E`/`FAILED`/`ERROR` lines and the
summary without duration). The module docstring of the test lists the exact rules.

Under the Airflow adapter every later refactoring step must reproduce these files exactly,
apart from documented additive JSON fields. After an intended change, re-record with

```
HEALER_UPDATE_GOLDEN=1 python -m pytest tests/unit/test_golden_snapshots.py
```

and review the diff of this folder in the same change.
