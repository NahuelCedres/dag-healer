# sfn-etl: dag-healer on AWS Step Functions

A small, deployable AWS SAM application to try dag-healer's Step Functions adapter on a real
account: the sales, customers and inventory batch jobs as three state machines whose steps are
Python Lambda functions. Two days of upstream exports make them succeed, then fail in the three
ways dag-healer tells apart:

| Pipeline | State machine | Failing state on day 2 | Error | Outcome |
|---|---|---|---|---|
| `sales_daily_etl` | `SalesDailyEtlStateMachine-<suffix>` | `TransformSales` | `KeyError: 'amount'` (`amount` renamed to `amt` upstream) | DATA: reported, the approved contract is violated |
| `customers_sync` | `customers-sync` | `TransformCustomers` | `ValueError: unconverted data remains: T08:30:00Z` (latent bug in `parse_signup_date`) | CODE: repaired, a fix of `src/etl/customers.py` and a regression test |
| `inventory_report` | `InventoryReportStateMachine-<suffix>` | `BuildReport` (inline Map `PerWarehouse`) | `KeyError: 'qty'` (`qty` renamed to `quantity`) | AMBIGUOUS: waits for a human, the contract is a draft |

The same files (minus this guide and `healer.yaml`) are dag-healer's offline test repository
for the adapter, `tests/unit/fixtures/repos/sfn_etl/`.

## Layout

- `template.yaml` - functions (Python 3.11, `app.handler`), the shared `EtlLayer` (`src/` with
  pandas), the state machines, the landing bucket, and the EventBridge rule -> SQS queue
  (`healer-sfn-events`, with a dead-letter queue) that dag-healer drains
- `statemachine/*.asl.json` - the definitions (`${...}` are `DefinitionSubstitutions`)
- `functions/<name>/app.py` - thin handlers: one step per invocation, chosen by `event["step"]`
- `src/etl/` - the job code; in Lambda it reads `s3://<bucket>/incoming/<file>` and writes
  `s3://<bucket>/processed/<name>.csv`
- `healer.yaml`, `healer/contracts/` - dag-healer's configuration and the draft data contracts
  (inferred from `fixtures/day1` with dag-healer's contract inference)
- `fixtures/day1/`, `fixtures/day2/` - the two daily drops; `fixtures/fake_llm/` - the scripts of
  the offline LLM provider
- `events/eventbridge-rule.json` - the event pattern of the rule, to reuse for state machines
  of other stacks
- `scripts/start_executions.sh` - starts the three executions of one day

## 1. Deploy

You need an AWS account, the AWS SAM CLI, the AWS CLI v2 and Python 3.11.

```bash
sam build
sam deploy --guided          # stack name sfn-etl, region eu-west-1, LandingBucketName=<a new, unique bucket name>
```

The stack outputs give everything the next steps need:

```bash
aws cloudformation describe-stacks --stack-name sfn-etl --query "Stacks[0].Outputs" --output table
BUCKET=$(aws cloudformation describe-stacks --stack-name sfn-etl \
  --query "Stacks[0].Outputs[?OutputKey=='LandingBucket'].OutputValue" --output text)
```

## 2. Day 1: everything succeeds

```bash
aws s3 cp fixtures/day1/ "s3://$BUCKET/incoming/" --recursive
scripts/start_executions.sh 2026-10-01
```

The three executions succeed (Step Functions console, or `aws stepfunctions list-executions`).

## 3. Point dag-healer at the stack

Install dag-healer with the AWS extra (`pip install 'dag-healer[aws]'`), then edit `healer.yaml`:

- `orchestrator.options.pipelines.*.state_machine`: the `SalesDailyEtlStateMachineArn` and
  `InventoryReportStateMachineArn` outputs (the names carry a generated suffix);
  `customers-sync` has a fixed name and needs no change;
- the bucket of the dataset locators, in `healer.yaml` and the contracts:
  `sed -i.bak "s/acme-landing/$BUCKET/" healer.yaml healer/contracts/*.yaml`;
- the queue: `export HEALER_SFN_QUEUE_URL=<the HealerEventsQueueUrl output>`;
- the test sandbox, where a repair runs pytest: build an image with Python 3.11, pytest and
  `src/requirements.txt`, with a non-root `USER`, on the worker's container engine (it is never
  pulled), and `export HEALER_SANDBOX_IMAGE=<its name>`. Without it, the CODE repair of
  `customers_sync` ends `repair_failed` and runs no code (see
  [docs/security.md](../../docs/security.md), section 4.3).

The Step Functions, CloudWatch Logs, Lambda and SQS clients use the default boto3 chain, or the
`profile`/`role_arn` of the options; the datasets in the bucket (the `s3://` locators of the
contracts) are read with the default chain. dag-healer only reads (executions, histories, Lambda
logs, the datasets in the bucket, the queue) unless a redrive is asked for.

Approve the two contracts that are meant to be enforced and check the setup:

```bash
dag-healer contracts approve sales_daily_etl --by "<your name>"
dag-healer contracts approve customers_sync --by "<your name>"
dag-healer doctor
```

## 4. Day 2: three failures

```bash
aws s3 cp fixtures/day2/ "s3://$BUCKET/incoming/" --recursive
scripts/start_executions.sh 2026-10-02
```

All three executions fail within a minute. EventBridge puts one message per failed execution on
the queue; `ListExecutions` finds them as well.

## 5. Let dag-healer handle them

```bash
dag-healer worker --once
dag-healer incidents list
```

`llm.provider` defaults to `fake` here: dag-healer classifies with its deterministic policy and
replays `fixtures/fake_llm` for the repair, without an API key. Set `HEALER_LLM_PROVIDER=anthropic`
(and `ANTHROPIC_API_KEY`) for real diagnoses, or `HEALER_LLM_PROVIDER=bedrock` for Claude on
Amazon Bedrock in your AWS account: then `llm.model` holds the Bedrock model or inference profile
ID (such as `eu.anthropic.claude-opus-5-5`) and `llm.aws_region` or `AWS_REGION` the region (the
commented lines of the `llm` block), the credentials come from the default chain rather than the
adapter's `profile` or `role_arn`, and their identity needs `bedrock:InvokeModel` on that model
([docs/bedrock.md](../../docs/bedrock.md)). The outcomes are the ones of the table above. The
CODE fix is a pull request (with `github.repo` and `GITHUB_TOKEN`; see
[docs/github/README.md](../../docs/github/README.md) for the token and the base branch's rules)
or a `PR.md`; it changes `src/etl/customers.py` and adds a test, and it reaches AWS only through
your deployment (`sam build && sam deploy`): a redrive reruns the code deployed at that time with
the original definition, so deploy the merged fix first.

## The manual live check

The steps above are dag-healer's live end-to-end check of the adapter (`HEALER_SFN_LIVE=1` in the
design notes). It is manual: dag-healer's automated tests never call AWS (they run against an
offline copy of this stack), and none of them drives a deployed stack yet. Expected: three
incidents, `sales_daily_etl` reported (DATA), `customers_sync` repaired
(CODE, the change limited to `src/etl/customers.py` plus the new test) and `inventory_report`
waiting for a human (AMBIGUOUS).

## Other state machines

To watch state machines of another stack, create a rule with the pattern of
`events/eventbridge-rule.json` (their ARNs in `detail.stateMachineArn`) and an SQS queue target,
or an API destination that posts to dag-healer's `/events/eventbridge` endpoint.

## Clean up

```bash
aws s3 rm "s3://$BUCKET" --recursive
sam delete --stack-name sfn-etl
```
