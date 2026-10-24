# sfn-etl

The sales, customers and inventory batch jobs as AWS Step Functions state machines whose
steps are Python Lambda functions, deployed with AWS SAM. This copy is the test repository of
dag-healer's Step Functions adapter; `examples/stepfunctions_repo/` is the same code with a
deployable `healer.yaml` and a guide.

## Layout

- `template.yaml` - the SAM template: the three functions (`Globals.Function`: Python 3.11,
  `app.handler`, the shared `EtlLayer`), the layer, the three state machines (`DefinitionUri` +
  `DefinitionSubstitutions`), the landing bucket and the EventBridge rule -> SQS queue that
  dag-healer reads failed executions from
- `statemachine/*.asl.json` - the state machine definitions
- `functions/<name>/app.py` - thin Lambda handlers: one ETL step per invocation, chosen by
  `event["step"]`
- `src/etl/` - the job code, packaged as the `EtlLayer` (`ContentUri: src/`, so it runs from
  `/opt/python/etl/`); `src/requirements.txt` adds pandas to the layer
- `healer.yaml`, `healer/contracts/` - the dag-healer configuration and the draft data contracts
- `fixtures/day1/`, `fixtures/day2/` - two daily drops of the upstream exports
- `fixtures/fake_llm/` - the scripts of dag-healer's offline LLM provider

In Lambda the jobs read `s3://<LANDING_BUCKET>/incoming/<file>` and write
`s3://<LANDING_BUCKET>/processed/<name>.csv`; without `LANDING_BUCKET` they use `data/incoming/`
and `data/processed/` locally. Tests import the shared code as `etl` (`pyproject.toml` puts
`src` on the pytest path, like the layer does in Lambda).

## The day-2 failures

`fixtures/day1/*.csv` is the drop every job succeeds on; the draft contracts were inferred from
it with dag-healer's contract inference. `fixtures/day2/*.csv` is the next day's drop:

| Pipeline | State machine | What changed on day 2 | Failing state | Error | Category |
|---|---|---|---|---|---|
| `sales_daily_etl` | `SalesDailyEtlStateMachine` | `amount` renamed to `amt`, some empty | `TransformSales` | `KeyError: 'amount'` in `src/etl/sales.py::transform_sales` | DATA (the approved contract requires `amount`) |
| `customers_sync` | `customers-sync` | some `signup_date` values are ISO-8601 timestamps | `TransformCustomers` | `ValueError: unconverted data remains: T08:30:00Z` in `src/etl/customers.py::parse_signup_date` | CODE (latent bug) |
| `inventory_report` | `InventoryReportStateMachine` | `qty` renamed to `quantity`, new `warehouse_code` | `BuildReport`, inside the `PerWarehouse` Map | `KeyError: 'qty'` in `src/etl/inventory.py::buildReport` | AMBIGUOUS (draft contract only) |

The tests approve the sales and customers contracts before they run the scenarios; the
inventory contract stays a draft. The expected CODE fix only touches
`src/etl/customers.py::parse_signup_date` (parse with `datetime.fromisoformat`), plus a new
regression test.
