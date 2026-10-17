# Test bench fixtures (day 2)

This folder is part of the dag-healer test bench, not of the legacy repo story.
`example_existing_repo/` plays the role of an existing Airflow repository that
dag-healer gets attached to: no tests, no contracts, slightly messy code.

`data/incoming/*.csv` is the **day 1** drop: every DAG succeeds on it and
`dag-healer init` profiles it to draft the contracts. `fixtures/day2/*.csv` is
the **next day** drop. The demo copies these files over `data/incoming/` and
triggers the three DAGs, which then fail in three different ways.

| DAG | Day 2 file | What changed | Failing task | Error | Expected category |
|-----|------------|--------------|--------------|-------|-------------------|
| `sales_daily_etl` | `sales.csv` | Upstream renamed `amount` to `amt`; several rows have an empty `amt` | `transform_sales` | `KeyError: 'amount'` in `src/etl/sales.py::transform_sales` | DATA |
| `customers_sync` | `customers.csv` | Same schema, no nulls; some `signup_date` values are now ISO-8601 timestamps (`2026-10-02T14:03:00Z`) mixed with plain dates | `transform` | `ValueError` raised by `datetime.strptime` in `src/etl/customers.py::parse_signup_date` | CODE |
| `inventory_report` | `inventory.csv` | New column `warehouse_code`; `qty` renamed to `quantity` | `build_report` | `KeyError: 'qty'` in `src/etl/inventory.py::buildReport` | AMBIGUOUS |

## 1. DATA - `sales_daily_etl`

The approved contract for `data/incoming/sales.csv` requires a non-null
`amount` column. On day 2 the column is missing (`missing_column`), so the
failure is the upstream's fault: dag-healer reports the violated rule with a
masked sample and alerts the team. No code change is expected.

## 2. CODE - `customers_sync`

The day 2 customers file satisfies the approved contract (same columns, same
logical types, no nulls, row count in band). The crash comes from a latent bug:
`parse_signup_date` hardcodes `"%Y-%m-%d"` and cannot parse timestamps that are
valid ISO-8601. The expected minimal fix touches only that function, for
example:

```python
def parse_signup_date(value):
    value = value.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).date()
```

dag-healer should add a pytest test that reproduces the failure (the repo has
no `tests/` folder, so a minimal one is created and called out in the PR),
apply the fix, run pytest and open a PR (or write `PR.md` + `fix.patch`).

## 3. AMBIGUOUS - `inventory_report`

The inventory contract stays in **draft** (nobody approved it), so there is no
agreed source of truth. Day 2 adds `warehouse_code` and renames `qty` to
`quantity`; `buildReport` fails with `KeyError: 'qty'`. It could be an upstream
mistake (DATA) or an intended schema change the code must follow (CODE), so the
incident waits for a human (`waiting_human`). Resolving it from the UI as
`data` or `code` resumes the pipeline on that branch.

## Resetting

Restore day 1 by copying the files back from git
(`git checkout -- data/incoming/`) and delete `data/processed/`.
