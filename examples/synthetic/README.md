# Synthetic example cases

Three more pipelines for the demo stack, each with its own synthetic data and its own way of
failing. They sit next to the base demo (`example_existing_repo/`) without changing it: one
case per category, none of them a repeat of the base three.

| DAG | Failing task | Day 2 | dag-healer ends with |
|---|---|---|---|
| `payments_settlement` | `settle_payments` | a new payment provider sends decimal-comma amounts (`1.234,50`) and some rows lose their currency | **DATA**, reported: `dtype_mismatch` on `amount`, `nulls_not_allowed` on `currency`, masked sample, alert |
| `orders_enrichment` | `enrich_orders` | the order system now exports cancelled orders (`items` 0, `total` 0.00), which the approved contract allows | **CODE**, repaired: failing test, minimal fix in `average_item_price`, pytest, `PR.md` |
| `fx_rates_publish` | `publish_rates` | nothing wrong with the data: the pricing service it publishes to refuses connections | **AMBIGUOUS** (infrastructure), waiting for a human |

## Run them

```bash
python scripts/tasks.py demo          # once: the base demo prepares .demo/repo and the stack
python scripts/synthetic_cases.py     # adds the three cases and plays their day 2
```

`scripts/synthetic_cases.py` writes the data (`.demo/synthetic/`), copies `dags/`, `src/` and
`fixtures/` into `.demo/repo` and commits them there, runs `dag-healer init` (existing
contracts are kept, the new ones are drafted) and approves the three new contracts. Then it
drops the day-2 files into `data/incoming/` (uncommitted, like an upstream delivery),
triggers the DAGs and waits until every incident has settled. Running it again is safe.

The base demo resets `.demo/repo` and the healer store, so run `synthetic_cases.py` again
after every `python scripts/tasks.py demo`.

## What to try in the UI

Open the Healer page (<http://localhost:8080/plugin/healer>) and the incident of each case
(the "Healer incident" tab of the failed task):

- **payments_settlement** - the two violated rules of the approved contract, the masked sample
  (`payer_name`, `payer_email` and `payer_phone` are hashed; amounts and currencies are not)
  and the alert in Mailpit (<http://localhost:8025>).
- **orders_enrichment** - the regression test dag-healer wrote first
  (`tests/test_orders_average_item_price.py`, red before the fix), the diff, the pytest
  output and the `PR.md`; the branch is in `.demo/repo` (`git branch --list "healer/*"`).
- **fx_rates_publish** - nothing to fix in data or code, so a human decides. Bring the pricing
  service back with a stand-in, clear the failed `publish_rates` task in Airflow (task
  instance -> Clear) and watch it succeed, then close the incident with "Ignore":

  ```bash
  python scripts/synthetic_cases.py pricing-up     # listens on 127.0.0.1:8907 in airflow-scheduler
  python scripts/synthetic_cases.py pricing-down   # back to the outage
  ```

  "Resolve as code" overrides the policy and starts a repair. The fake LLM has no script for
  this task, so the repair ends in `repair_failed`, the way a real model with nothing to fix
  would.

## The data

`generate.py` writes every file from one fixed seed, so each run is byte-identical:

| File | Day 1 | Day 2 |
|---|---|---|
| `payments.csv` | 600 payments from two providers | 640 payments; every 8th row from the new provider `euro-pay` |
| `orders.csv` | 500 completed or shipped orders | 520 orders; every 29th one cancelled |
| `fx_rates.csv` | 30 days x 8 currencies (USD base) | the same window, one day later |

People are synthetic: names come from short fixed lists, e-mail addresses use the reserved
`example.com`, `example.org` and `example.net` domains and phone numbers the fictional
`555-01xx` range. To write the files somewhere else:

```bash
python examples/synthetic/generate.py --day1 out/day1 --day2 out/day2
```

## Files

```
dags/                 the three DAGs (copied into .demo/repo/dags/)
src/etl/              their modules; they reuse src/etl/common.py of example_existing_repo
fixtures/fake_llm/    scripts of the offline fake LLM for the CODE case (test, then fix)
generate.py           the synthetic data generator
```

The fake LLM scripts only matter with the default `HEALER_LLM_PROVIDER=fake`. With
`HEALER_LLM_PROVIDER=anthropic` and your `ANTHROPIC_API_KEY` in `docker/.env`, Claude writes the
test and the fix instead.
