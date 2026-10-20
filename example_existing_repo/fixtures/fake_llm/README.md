# Fake LLM scripts

dag-healer talks to Claude through a provider interface. Besides the real
Anthropic provider there is an offline **fake provider** (`llm.provider: fake`
in `healer.yaml`, or `HEALER_LLM_PROVIDER=fake`). It exists so that CI, the unit
tests and the demo run without an `ANTHROPIC_API_KEY`, without network access
and with fully deterministic results.

The fake provider does two things:

- **Classification** - it reads the `<signals>` JSON block that dag-healer
  appends to every classification prompt and answers with the category of the
  deterministic policy hint (confidence 0.9 for `data` / `code`, 0.5 for
  `ambiguous`) and a diagnosis built from the signals. No script is needed.
- **Repair sessions** - instead of letting a model decide which tools to call,
  it replays the YAML scripts in this folder.

## Where scripts are read from

1. `$HEALER_FAKE_LLM_DIR`, when set;
2. otherwise `<repo.path>/fixtures/fake_llm` (this folder, once the demo has
   copied `example_existing_repo/` into place).

Every `*.yaml` / `*.yml` file is a script. Invalid files are skipped with a
warning in the worker log.

## Script format

```yaml
name: customers_sync_test      # optional, defaults to the file name
match:                         # which repair session this script answers
  dag_id: customers_sync
  task_id: transform
  exception_type: ValueError   # compared by short name: builtins.ValueError matches
  phase: test                  # "test" (write the failing test) or "fix" (change the code)
steps:                         # tool calls, executed in order
  - tool: write_file
    input:
      path: tests/test_customers_signup_date.py
      content: |
        ...
  - tool: run_tests
    input: {paths: [tests/test_customers_signup_date.py]}
final_text: >-                 # optional: what the "model" says at the end
  Added a regression test ...
```

`match` keys are `dag_id`, `task_id`, `exception_type` and `phase`. A key that is
left out matches anything, and a list matches any of its values. When several
scripts match, the one with the most matching keys wins (ties: first file name
in alphabetical order). Without a matching script the session ends with "The
fake LLM provider has no script for this scenario." and makes no tool calls, so
the repair fails the same way a real model that cannot help would.

`steps` call the repair tools by name with `input` as keyword arguments:
`list_files(path, pattern)`, `read_file(path, start_line, end_line)`,
`search(pattern, path)`, `write_file(path, content)`,
`replace_in_file(path, old, new)` (exactly one occurrence of `old`) and
`run_tests(paths)`. The tools enforce the same guardrails as with Claude: in the
`test` phase only the tests folder can be written, in the `fix` phase only
`repo.allowed_paths`, and protected files (`healer.yaml`, `fixtures/**`,
`data/**`, `.env*`, ...) never. A refused or failing call is recorded as an
error and the script goes on; add `stop_on_error: true` to a step to end the
script there.

Tip: keep `replace_in_file` replacements to single lines of `old` text so they
match whatever line endings the checkout uses.

## Scripts in this folder

| Script | Phase | What it does |
|--------|-------|--------------|
| `customers_sync_test.yaml` | test | Writes `tests/test_customers_signup_date.py`: `parse_signup_date("2026-10-02T14:03:00Z")` must return `date(2026, 10, 2)` (fails before the fix with `ValueError`) and plain dates must keep working. |
| `customers_sync_fix.yaml` | fix | Minimal change to `src/etl/customers.py::parse_signup_date`: parse with `datetime.fromisoformat` (a trailing `Z` becomes `+00:00`) and still return a date; then runs the new test and the whole suite. |
| `inventory_report_test.yaml` | test | Writes `tests/test_inventory_build_report.py`: `buildReport` fed (through `monkeypatch`, no data files) with the day 2 schema (`quantity`, `warehouse_code`) must still aggregate stock per SKU (fails before the fix with `KeyError: 'qty'`); the old `qty` schema must keep working. |
| `inventory_report_fix.yaml` | fix | Minimal change to `src/etl/inventory.py::buildReport`: rename `quantity` back to `qty` when only the new name is present; then runs the new test and the whole suite. |

The `customers_sync` scripts cover the CODE scenario of the demo (see
`../README.md`). The `inventory_report` scripts are only used when a human
resolves the AMBIGUOUS incident as **code** (the upstream rename is intended),
so that the keyless demo also resumes up to a PR / `PR.md`. The DATA scenario
(`sales_daily_etl`) never reaches a repair session and needs no script.
