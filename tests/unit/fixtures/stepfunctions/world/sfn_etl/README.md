# The `sfn_etl` offline world

`../sfn_etl.json` is an AWS account in a file: the state of `eu-west-1` / `123456789012` one hour
after the day-2 executions of the `sfn_etl` repository
(`tests/unit/fixtures/repos/sfn_etl`) failed. `dag_healer.stepfunctions.testing.OfflineAws.load`
turns it into botocore clients that answer without credentials or network.

| Pipeline | State machine | Execution | Failing state | Error |
|---|---|---|---|---|
| `sales_daily_etl` | `SalesDailyEtlStateMachine-AbC123` | `sales-2026-10-02` | `TransformSales` | `KeyError: 'amount'` |
| `customers_sync` | `customers-sync` | `customers-2026-10-02` | `TransformCustomers` | `ValueError: unconverted data remains: T08:30:00Z` |
| `inventory_report` | `InventoryReportStateMachine-XyZ789` | `inventory-2026-10-02` | `BuildReport` (inline Map `PerWarehouse`, iteration 0) | `KeyError: 'qty'` |

What it holds:

- **State machines**: the repository's ASL with the deployed function ARNs in place of the
  `DefinitionSubstitutions` (what `DescribeStateMachine` returns after `sam deploy`).
- **Functions**: `sfn-etl-SalesFunction-1A2B3C4D5E6F` and `sfn-etl-CustomersFunction-7G8H9I0J1K2L`
  log as Text to `/aws/lambda/<name>`; `sfn-etl-InventoryFunction-3M4N5O6P7Q8R` logs as JSON to
  `/sfn-etl/inventory` (`LoggingConfig`).
- **Executions** with complete histories. Sales and customers call the optimised
  `arn:aws:states:::lambda:invoke` integration (`Task*` events); `BuildReport` uses a Lambda ARN
  as its `Resource` (`LambdaFunction*` events) inside the Map (`MapIteration*` events).
- **Lambda log events**: a cold start (`INIT_START`), the successful extract step, then the
  failing step in the same log stream. The customers execution input and its log carry the
  address `alice.pii@example.com`: it must never reach `native` or a log record of the adapter.
- **S3 objects**: the day-2 exports in `s3://acme-landing/incoming/` (`s3/`).
- **SQS**: the queue of the EventBridge rule with one message, the `FAILED` status change of the
  customers execution (`sqs/customers-sync-failed.json`).

How it was made: the Lambda steps of the repository ran in-process on the day-2 exports, the
way Lambda runs them (`/var/task` = `functions/<name>`, `/opt/python` = `src`, `LANDING_BUCKET`
set, the S3 client stubbed in memory). Their real tracebacks and log lines, with the paths
rewritten to the Lambda layout (`/var/task`, `/opt/python`, `/var/lang/lib/python3.11`), became
the `TaskFailed` causes and the CloudWatch log events. `tests/unit/stepfunctions/test_repos.py`
runs the steps again and checks that every frame of the repository's files still points at the
same line and code.

Shapes taken from the documentation and the Lambda Python runtime, to confirm with a real
recording [U]: the error payload of the Python 3.11 runtime (`errorMessage`, `errorType`,
`requestId`, `stackTrace` as preformatted strings); the JSON record of an unhandled error
(`log_level: ERROR` and the error payload); the log stream name of a custom log group
(`<date>/<function name>[$LATEST]<id>`); an inline Map that fails with the error and cause of its
failed iteration.
