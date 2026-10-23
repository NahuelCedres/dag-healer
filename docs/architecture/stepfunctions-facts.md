# Fact sheet: AWS Step Functions (plus a short Dagster/Prefect section) for building dag-healer's orchestrator seam

Research notes, checked against the cited sources in October 2026.

**Tags used below**
- **[V]**: confirmed in the cited source.
- **[I]**: inferred from verified facts.
- **[U]**: not verified; check before relying on it.

---

## 1. EventBridge "Step Functions Execution Status Change"

Source: https://docs.aws.amazon.com/step-functions/latest/dg/eventbridge-integration.html

- **[V] Envelope and coverage.** `source` is `aws.states` and `detail-type` is `"Step Functions Execution Status Change"`. Events go to the default event bus automatically. **Only Standard workflows emit these events.** Express workflows must be monitored through CloudWatch Logs.
- **[V] Delivery.** Delivery is "best-effort", and "Events might be delivered out of order".
- **[V] `detail` fields:**
  - `executionArn`, `stateMachineArn`, `name`
  - `status`: `RUNNING | SUCCEEDED | FAILED | TIMED_OUT | ABORTED | PENDING_REDRIVE`
  - `startDate`, `stopDate` (integer epoch milliseconds; `stopDate` is null while running)
  - `input`, `inputDetails{included}`, `output`, `outputDetails{included}`
  - `stateMachineVersionArn`, `stateMachineAliasArn`
  - `redriveCount`, `redriveDate`
  - `redriveStatus`: `NOT_REDRIVABLE | REDRIVABLE | REDRIVE_IN_PROGRESS`
  - `redriveStatusReason`
  - **`error` and `cause` are present.** Example: FAILED gives `"error":"ErrorCode","cause":"An error occurred."`, TIMED_OUT gives `"error":"States.Timeout"`.
- **[V] No failing state in the event.** The event does not name the state that failed. You have to call GetExecutionHistory to find it.
- **[V] Payload limits.** If the escaped input plus output exceeds 248 KiB, the input is dropped. If the output alone exceeds 248 KiB, the output is dropped. Use `inputDetails`/`outputDetails` to tell, and DescribeExecution for the full payload (Standard only).
- **[V] Example rule pattern:** `{"source":["aws.states"],"detail-type":["Step Functions Execution Status Change"],"detail":{"status":["FAILED","TIMED_OUT"],"stateMachineArn":[...]}}`
- **[V] Only one event type is listed for Step Functions.** There are no per-state or Map Run events, so failures that a `Catch` handles are invisible here.
- **[V] Push target option: API destinations** (https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-api-destinations.html). An HTTPS endpoint is called through a "connection" that holds the auth settings. The endpoint must answer within 5 seconds. Event-bus retries default to 24 hours and 185 attempts, then go to a DLQ or are dropped. 401/407/409/429/5xx are retried.
- **[U]** Whether Standard child executions started by a Distributed Map emit this event, and what `stateMachineArn` looks like for them.

## 2. GetExecutionHistory and the failure events

Sources:
- https://docs.aws.amazon.com/step-functions/latest/apireference/API_GetExecutionHistory.html
- https://docs.aws.amazon.com/step-functions/latest/apireference/API_HistoryEvent.html

**Request and paging**
- **[V] Request:** `executionArn`, `includeExecutionData` (default true; controls the input/output of events), `maxResults` (default 100, max 1000, "only an upper limit"), `nextToken`, `reverseOrder`.
- **[V] Paging:** page tokens expire after **24 hours** (HTTP 400 InvalidToken). The call is **not supported for EXPRESS** state machines.
- **[V] Errors:** ExecutionDoesNotExist, InvalidArn, InvalidToken, and the Kms* errors. A KMS-encrypted state machine needs `kms:Decrypt`.

**Event shape**
- **[V] `HistoryEvent`** has `id` (sequential from 1), `previousEventId`, `timestamp`, `type`, and one `*EventDetails` object.

**Failure-related event types [V]**
- `ExecutionFailed`, `ExecutionTimedOut`, `ExecutionAborted`
- `TaskFailed`, `TaskStartFailed`, `TaskSubmitFailed`, `TaskTimedOut`, `TaskStateAborted`
- `LambdaFunctionFailed`, `LambdaFunctionStartFailed`, `LambdaFunctionScheduleFailed`, `LambdaFunctionTimedOut`
- `ActivityFailed`, `ActivityScheduleFailed`, `ActivityTimedOut`
- `MapRunFailed`, `MapRunAborted`, `MapIterationFailed`, `MapIterationAborted`, `MapStateFailed`, `MapStateAborted`
- `ParallelStateFailed`, `ParallelStateAborted`
- `FailStateEntered`, `EvaluationFailed`
- Redrive markers: `ExecutionRedriven`, `MapRunRedriven`

**Detail fields [V]**
- `*FailedEventDetails`, `*TimedOutEventDetails`, `executionAbortedEventDetails`: `error` and `cause`.
- `taskFailedEventDetails`: `error` (max 256 chars), `cause` (max 32768 chars), `resource` ("the action", e.g. `invoke`), `resourceType` ("the service name", e.g. `lambda`). See https://docs.aws.amazon.com/step-functions/latest/apireference/API_TaskFailedEventDetails.html.
- `evaluationFailedEventDetails`: `error`, `cause`, `location`, `state`.
- `stateEnteredEventDetails` and `stateExitedEventDetails`: `name`, `input`/`output` plus `*Details.truncated`.
- `mapIteration*EventDetails`: `index` and `name` (the parent Map state's name).
- `mapRunStartedEventDetails`: `mapRunArn`.
- `executionRedrivenEventDetails`: `redriveCount`.
- `taskScheduledEventDetails`: `resource`, `resourceType`, `region`, `parameters`, `timeoutInSeconds`, `heartbeatInSeconds`.

**Finding the failing state name**
- **[V]** The doc's sample has `PassStateExited` with `previousEventId: 2`, which points at `PassStateEntered`. So the link points to the event that caused this one, not simply the event before it.
- **[V]** State names "MUST be unique within the scope of the entire state machine" and are at most 80 characters (https://states-language.net/spec.html). So `(state machine, state name)` identifies a step.
- **[I] Recommended approach:**
  1. Call with `reverseOrder=true` and a small `maxResults`.
  2. Find the last `*Failed` or `*TimedOut` event of a Task, Lambda function or Activity.
  3. Follow `previousEventId` back to its `TaskStateEntered` to get `name`.
  4. If the root is `MapRunFailed`, switch to the child executions (section 3).
- **[I] Legacy versus optimized Lambda calls.** A Task whose Resource is a bare Lambda ARN produces `LambdaFunction*` events (the details carry `resource` = function ARN). The optimized `arn:aws:states:::lambda:invoke` produces `Task*` events with `resourceType`/`resource`. The adapter must handle both.
- **[V] Rate limits** (https://docs.aws.amazon.com/step-functions/latest/dg/service-quotas.html): GetExecutionHistory bucket 400, refill 20/s. DescribeExecution 300/15 (large regions) or 250/10. ListExecutions 200/5 or 100/2.
- **[V] Size and retention limits:** 25,000 history events per Standard execution (a hard limit; the execution fails when it is hit). History is retained 90 days after close, reducible to 30. The input/output limit is 256 KiB.

## 3. Map, Distributed Map and child executions

**Inline Map**
- **[V]** Iterations appear inside the same history as `MapIteration*` events with `index` (the array position) and `name`.
- **[V]** Inside a Map, the Context object exposes `Map.Item.Index`, `Key`, `Value` and `Source`. They can only be injected through `ItemSelector` (https://docs.aws.amazon.com/step-functions/latest/dg/input-output-contextobject.html).

**Distributed Map** (https://docs.aws.amazon.com/step-functions/latest/dg/state-map-distributed.html)
- **[V] Child executions.** Each child execution has its own separate history. `ProcessorConfig.ExecutionType` is STANDARD or EXPRESS. Distributed mode is not supported inside Express workflows. Default and maximum concurrency is 10,000.
- **[V] Labels.** `Label` (at most 40 characters, unique in the definition) goes into the Map Run ARN, e.g. `arn:aws:states:region:acct:mapRun:demoWorkflow/demoLabel:3c39a231-...`.
- **[V] Child ARNs.** Child state machines use the labelled ARN `arn:...:stateMachine:<name>/<MapRunLabel or UUID>`. The IAM examples show child executions as `arn:aws:states:region:acct:execution:<stateMachineName>/<mapRunLabel>:*`.
- **[V] Thresholds.** `ToleratedFailurePercentage` and `ToleratedFailureCount`. Exceeding them raises `States.ExceedToleratedFailureThreshold`.
- **[V] Retry on a Distributed Map** applies to all child executions, and **"When you retry a Map state, it creates a new Map Run."**

**Listing and describing**
- **[V] ListExecutions** (https://docs.aws.amazon.com/step-functions/latest/apireference/API_ListExecutions.html) takes `mapRunArn` *or* `stateMachineArn`, plus `statusFilter` (`PENDING_REDRIVE` requires `mapRunArn`), `redriveFilter` (`REDRIVEN | NOT_REDRIVEN`), and `maxResults` (max 1000).
  - It is eventually consistent.
  - It is not supported for EXPRESS, except for Express children listed by `mapRunArn`.
  - Items include `executionArn`, `mapRunArn`, `itemCount`, `redriveCount`, `status`, and the dates.
- **[V] DescribeMapRun** (https://docs.aws.amazon.com/step-functions/latest/apireference/API_DescribeMapRun.html) returns:
  - `status`: `RUNNING | SUCCEEDED | FAILED | ABORTED`
  - `itemCounts` and `executionCounts`: pending, running, succeeded, failed, timedOut, aborted, total, resultsWritten, failuresNotRedrivable, pendingRedrive
  - tolerated-failure settings, `redriveCount`, `redriveDate`, `executionArn` (the parent)
- **[V] DescribeExecution** works on Express executions "unless a Map Run dispatched them" (i.e. only those), and returns `mapRunArn`.
- **[V] DescribeStateMachineForExecution** returns the **definition the execution actually used**, plus `label`, `mapRunArn`, `revisionId`, `loggingConfiguration` and `roleArn`. It is not supported for EXPRESS (https://docs.aws.amazon.com/step-functions/latest/apireference/API_DescribeStateMachineForExecution.html).

**Item identity and retention** (https://docs.aws.amazon.com/step-functions/latest/dg/concepts-examine-map-run.html)
- **[V]** Item status follows its child execution. If one item in a batch fails, the whole child fails.
- **[V]** Child executions are retained for 90 days.
- **[I]** So the "map index" for a Distributed Map is the **child execution name**, a string, not an item index.

## 4. Retry, Catch, and how attempts appear

Source: https://docs.aws.amazon.com/step-functions/latest/dg/concepts-error-handling.html

**Retry**
- **[V] Retrier fields:**
  - `ErrorEquals` (required)
  - `IntervalSeconds` (default 1)
  - `MaxAttempts` (default 3; 0 means never retry)
  - `BackoffRate` (default 2.0)
  - `MaxDelaySeconds`
  - `JitterStrategy` (`FULL | NONE`)
- **[V]** Retriers apply "across all visits ... in the context of a single-state execution". Retries count as state transitions.

**Catch**
- **[V]** Catch fields are `ErrorEquals`, `Next` and `ResultPath`. The error output has `Error` and `Cause`.
- **[V]** Catch is available on Task, Parallel and Map.
- **[V]** A caught error lets the execution continue, and possibly SUCCEED.

**Built-in error names [V]**
- `States.ALL` does not catch `States.DataLimitExceeded` or `States.Runtime`.
- `States.TaskFailed` (wildcard except Timeout), `States.Timeout`, `States.HeartbeatTimeout`, `States.Permissions`, `States.Runtime` (not retriable), `States.DataLimitExceeded`, `States.ExceedToleratedFailureThreshold`, `States.ItemReaderFailed`, `States.ResultWriterFailed`, `States.Http.Socket`.
- Lambda errors: `Lambda.ServiceException`, `Lambda.SdkClientException`, `Lambda.AWSLambdaException`, `Lambda.TooManyRequestsException`, `Lambda.Unknown`, and `Sandbox.Timedout` for timeouts in newer runtimes.
- The best-practices page recommends retrying Lambda service exceptions (https://docs.aws.amazon.com/step-functions/latest/dg/sfn-best-practices.html).

**How attempts show up**
- **[V]** The Context object has `State.RetryCount`, `Execution.RedriveCount`, `Execution.Id`, `Execution.Name` and `StateMachine.Id`. These can be passed into a Lambda payload with `$$.Execution.Id` (JSONPath) or `$states.context.Execution.Id` (JSONata).
- **[V]** The console shows a "Retry" tab with each attempt.
- **[I]** In the history, each retry shows up as another `TaskScheduled → TaskStarted → TaskFailed` cycle under the same `TaskStateEntered`. An attempt is terminal when no further `TaskScheduled` follows it.
- **[V] Catch-and-swallow cases still appear in logs:** at log level ERROR, `TaskFailed`, `LambdaFunctionFailed`, `MapIterationFailed` and similar are logged even when the execution succeeds (https://docs.aws.amazon.com/step-functions/latest/dg/cw-logs.html).

## 5. RedriveExecution, StartExecution, and Express limits

**RedriveExecution** (https://docs.aws.amazon.com/step-functions/latest/dg/redrive-executions.html, https://docs.aws.amazon.com/step-functions/latest/apireference/API_RedriveExecution.html)
- **[V] What it does.** It continues from the unsuccessful step with **the same input, the same definition and the same execution ARN**, including the same version or alias. Successful steps are not rerun.
- **[V] Changed definition.** "Because redriven executions use the same state machine definition, you must start a new execution if you update your state machine definition."
- **[V] Eligibility:**
  - Standard only.
  - Status is not SUCCEEDED.
  - Within **14 days** of completion.
  - Under the 1-year maximum open time.
  - Fewer than **24,999** history events.
  - Started on or after 2023-11-15.
- **[V] Timeouts and retries are reset.** The state-machine timeout and the redriven Task's `TimeoutSeconds` are reset to 0. Retry counters are reset to 0.
- **[V] Per-state behaviour:**
  - Parallel and Inline Map rerun only failed branches or iterations, unless the cause was `States.DataLimitExceeded`.
  - A Distributed Map redrives its unsuccessful children: Standard children through RedriveExecution, Express children through **StartExecution**.
  - A Map Run can be redriven at most 1000 times.
- **[V] Request and errors.** The request is `{executionArn, clientToken}`. `clientToken` is idempotent for the last 10 tokens, for 15 minutes. The response is `{redriveDate}`. Errors: ExecutionNotRedrivable, ExecutionLimitExceeded, ValidationException.
- **[V] Rate limit:** bucket 1300, refill 300/s in large regions; 800/150 elsewhere.

**StartExecution** (https://docs.aws.amazon.com/step-functions/latest/apireference/API_StartExecution.html)
- **[V] Idempotency.** It is idempotent for STANDARD when name and input are the same as a running execution. A closed execution or different input gives `ExecutionAlreadyExists`. A name can be reused 90 days after close.
- **[V] Express.** Not idempotent, and names can be reused immediately.
- **[V] Limits.** Input is at most 256 KiB. Names are at most 80 characters; use `[0-9A-Za-z_-]` if you want CloudWatch logging to work.
- **[V] Versions and aliases.** It accepts version or alias ARNs. An unqualified ARN uses the latest revision.
- **[V]** The `ExecutionLimitExceeded` error applies at 1,000,000 open Standard executions per account and region.

**Express limits** (https://docs.aws.amazon.com/step-functions/latest/dg/concepts-standard-vs-express.html)
- **[V]** Maximum 5 minutes.
- **[V]** At-least-once (asynchronous) or at-most-once (synchronous).
- **[V]** No `.sync` or `.waitForTaskToken`.
- **[V]** No GetExecutionHistory, no ListExecutions or DescribeExecution (except Map Run children), no redrive, and no EventBridge events.
- **[V]** History exists only in CloudWatch Logs, which "are delivered on a best-effort basis".
- **[V]** The console keeps no definition history for Express.

## 6. Logs and correlation

**Step Functions' own logs** (https://docs.aws.amazon.com/step-functions/latest/dg/cw-logs.html)
- **[V] Settings.** `LoggingConfiguration{level: ALL|ERROR|FATAL|OFF, includeExecutionData, destinations[cloudWatchLogsLogGroup.logGroupArn]}`. Logging is off by default for Standard workflows and for anything created via API or CloudFormation.
- **[V] Log group naming.** Use the `/aws/vendedlogs/states/...` prefix to avoid the 5,120-character CloudWatch Logs resource-policy limit.
- **[V] Truncation.** Payloads are truncated, with `inputDetails`/`outputDetails`/`assignedVariablesDetails`.
- **[V] Record fields.** Logs Insights examples use the fields `execution_arn`, `event_timestamp` and `type` (https://docs.aws.amazon.com/step-functions/latest/dg/troubleshooting-express-workflows.html).
- **[U]** The full record schema (`id`, `previous_event_id`, `details`, `redrive_count`).

**Lambda logs**
- **[V] Log group.** The default is `/aws/lambda/<function-name>`. It can be overridden by `LoggingConfig.LogGroup` (with `LogFormat` `JSON|Text`), which GetFunction returns in `Configuration.LoggingConfig` (https://docs.aws.amazon.com/lambda/latest/api/API_LoggingConfig.html, https://docs.aws.amazon.com/lambda/latest/api/API_GetFunction.html). Logs can take 5 to 10 minutes to appear (https://docs.aws.amazon.com/lambda/latest/dg/monitoring-cloudwatchlogs.html).
- **[V] Stream names and request IDs.** Streams are named like `2025/08/31/[$LATEST]<instance-id>`, one per execution environment. The text format has `START/END/REPORT RequestId: <id>` lines. The JSON format has `timestamp`, `level`, `message` and `requestId`, and exceptions add `errorType`, `errorMessage`, `stackTrace` and `location` (https://docs.aws.amazon.com/lambda/latest/dg/python-logging.html).
- **[V] Error payload.** `{errorMessage, errorType, stackTrace}`. For Python, `stackTrace` is a list of `[file, line, function, code]`, for example `"/var/task/lambda_function.py", 3, "lambda_handler", ...` (https://docs.aws.amazon.com/apigateway/latest/developerguide/handle-errors-in-lambda-integration.html).
- **[V] Error signalling.** Errors come back as HTTP 200 with an `X-Amz-Function-Error` header (https://docs.amazonaws.cn/en_us/lambda/latest/dg/python-exceptions.html).
- **[V] Optimized-integration success output** includes `SdkResponseMetadata.RequestId` and the `x-amzn-RequestId` header (https://docs.aws.amazon.com/step-functions/latest/dg/connect-lambda.html).
- **[U]** Whether the TaskFailed `cause` contains a `requestId`.
- **[I] Correlation approach.** Use `FilterLogEvents` on the function's log group between the `TaskStarted` and `TaskFailed` timestamps, filtered by `errorType`. Or have functions log `$$.Execution.Id`, which is much stronger.
- **[V] FilterLogEvents** (https://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_FilterLogEvents.html): `logGroupName|logGroupIdentifier`, `logStreamNames|logStreamNamePrefix`, `startTime`/`endTime` in ms, `filterPattern`, `limit` ≤ 10,000. Pages can be empty while `nextToken` is still present. Tokens expire after 24 hours.

**ECS, Batch and Glue**
- **[V] ECS.** The stream is `prefix-name/container-name/ecs-task-id` (https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-properties-ecs-taskdefinition-logconfiguration.html). With `ecs:runTask.sync`, a non-empty `Failures` field fails the task with `AmazonECS.Unknown` (https://docs.aws.amazon.com/step-functions/latest/dg/connect-ecs.html).
- **[V] Batch.** The default group is `/aws/batch/job` (the userguide text omits the leading slash: `aws/batch/job`) and the stream is `prefix/default/ecs-task-id`. `DescribeJobs` returns `container.logStreamName` and `exitCode`, with one stream per attempt (https://docs.aws.amazon.com/batch/latest/userguide/using_awslogs.html, https://docs.aws.amazon.com/batch/latest/APIReference/API_ContainerDetail.html).
- **[V] Glue.** `/aws-glue/jobs/error` holds system and GlueLogger output; `/aws-glue/jobs/output` holds stdout and stderr. Streams are `<jobRunId>-driver` and `<jobRunId>-executorNum`. The `.sync` response includes `JobRunId` and `JobName`, and Step Functions polls `glue:GetJobRun` (https://docs.aws.amazon.com/glue/latest/dg/monitor-continuous-logging.html, https://docs.aws.amazon.com/step-functions/latest/dg/connect-glue.html).
- **[U]** The exact content of `cause` for `.sync` failures. It is probably the describe output, but this is not documented.

## 7. Mapping states to source code

**From the definition to a resource**
- **[V] ASL.** A Task `Resource` is a URI that identifies the task. For Lambda it is either a function ARN (legacy form) or `arn:aws:states:::lambda:invoke` with `FunctionName` in `Parameters`/`Arguments` (https://states-language.net/spec.html, connect-lambda page above).
- **[V] DescribeStateMachineForExecution** returns the definition that was actually run (section 3).

**CloudFormation and SAM**
- **[V] CloudFormation `AWS::StepFunctions::StateMachine`:** `Definition`, `DefinitionString`, `DefinitionS3Location`, `DefinitionSubstitutions` (`${key}` placeholders, e.g. `Resource: "${HelloFunction}"`), `LoggingConfiguration`, `StateMachineType`. Ref returns the ARN (https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-stepfunctions-statemachine.html).
- **[V] SAM `AWS::Serverless::StateMachine`:** `Definition`/`DefinitionUri` (local `*.asl.json`) plus `DefinitionSubstitutions` such as `MyFunctionArn: !GetAtt MyFunction.Arn` (https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/sam-resource-statemachine.html).
- **[V] SAM `AWS::Serverless::Function`:** `CodeUri` (local path or S3; intrinsic functions are not parsed), `Handler`, `InlineCode`, `ImageUri`/`PackageType`. If `FunctionName` is omitted, "a unique name is generated" (https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/sam-resource-function.html).
- **[I]** Python handler `pkg.mod.fn` resolves to `<CodeUri>/pkg/mod.py`. `/var/task/...` stack frames map to the `CodeUri` root.

**CDK**
- **[V]** The synthesized `AWS::Lambda::Function` carries `Metadata` keys `aws:asset:path` (the local asset path) and `aws:asset:property` (`Code`). This is on by default (`aws:cdk:enable-asset-metadata`, `--asset-metadata`).
- **[V]** The assets are *staged copies* in `cdk.out`. They point at the real source only when `aws:cdk:disable-asset-staging` is set (absolute paths, for SAM CLI). Sources: https://github.com/aws/aws-cdk/blob/main/packages/aws-cdk-lib/aws-s3-assets/README.md, https://github.com/aws/aws-cdk/blob/main/packages/aws-cdk-lib/cx-api/lib/app.ts, https://github.com/aws/aws-cdk/blob/main/packages/aws-cdk-lib/core/lib/asset-staging.ts

**Terraform**
- **[V] `aws_lambda_function`:** `filename` | `s3_bucket`/`s3_key` | `image_uri`, `handler`, `source_code_hash`, `logging_config.log_group` (https://raw.githubusercontent.com/hashicorp/terraform-provider-aws/main/website/docs/r/lambda_function.html.markdown).
- **[V] `archive_file`:** `source_dir`/`source_file`, `output_path`, `output_base64sha256` (https://raw.githubusercontent.com/hashicorp/terraform-provider-archive/main/docs/data-sources/file.md).
- **[V] `aws_sfn_state_machine`:** `definition` (the docs example uses a heredoc with `${aws_lambda_function.lambda.arn}`), `type`, `logging_configuration` (https://raw.githubusercontent.com/hashicorp/terraform-provider-aws/main/website/docs/r/sfn_state_machine.html.markdown).

**Serverless Framework**
- **[V]** `functions.<name>.handler` is file plus export, e.g. `handler.functionOne` (https://www.serverless.com/framework/docs/providers/aws/guide/functions).
- **[V]** The logical ID is `{normalizedFunctionName}LambdaFunction`, e.g. `HelloLambdaFunction` (https://www.serverless.com/framework/docs/providers/aws/guide/resources).
- **[V]** In the serverless-step-functions plugin, `stepFunctions.stateMachines.<n>.definition` refers to functions with `Fn::GetAtt: [hello, Arn]` (https://github.com/serverless-operations/serverless-step-functions).
- **[U]** The default deployed name `service-stage-function` (forum source only: https://forum.serverless.com/t/function-naming-convention/8717).

**Mapping from the deployed side**
- **[V]** CloudFormation adds the tags `aws:cloudformation:logical-id`, `stack-id` and `stack-name`, though "propagation ... varies by resource type" (https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-properties-resource-tags.html).
- **[V]** GetFunction returns `Tags` only if the caller has `lambda:ListTags`. It also returns `Code.Location` (a download link valid 10 minutes), `CodeSha256`, `Handler` and `Runtime`.

## 8. IAM

**[V] Permission levels** (https://docs.aws.amazon.com/step-functions/latest/dg/concept-create-iam-advanced.html):
- **Service level, `arn:aws:states:*:*:*`:** ListStateMachines, ValidateStateMachineDefinition.
- **State-machine level, `...:stateMachine:<prefix>*`:** DescribeStateMachine, StartExecution, ListExecutions, TestState, RevealSecrets.
- **Execution level, `...:execution:*:<prefix>*`:** DescribeExecution, DescribeStateMachineForExecution, GetExecutionHistory, StopExecution.

**[V] Redrive ARNs:**
- Execution: `states:RedriveExecution` on `arn:aws:states:<r>:<acct>:execution:<sm>:*`.
- Map Run: `...:execution:<sm>/<mapRunLabel>:*` (https://docs.aws.amazon.com/step-functions/latest/dg/iam-policies-eg-dist-map.html).

**[V] Tags.** Executions inherit the state machine's tags for tag-based authorization (`aws:ResourceTag/...` conditions). This lets you split read and write per state machine by tag (sfn-best-practices page, tagging section).

**[I] Proposed least-privilege policies:**
- **Read role:**
  - `states:ListExecutions`, `states:DescribeStateMachine`: stateMachine
  - `states:DescribeExecution`, `states:GetExecutionHistory`, `states:DescribeStateMachineForExecution`: execution
  - `states:DescribeMapRun`, `states:ListMapRuns`: mapRun/execution
  - `logs:FilterLogEvents`, `logs:StartQuery`, `logs:GetQueryResults`: specific log groups
  - `lambda:GetFunction`, `lambda:ListTags`
  - `batch:DescribeJobs`, `ecs:DescribeTasks`, `glue:GetJobRun`
  - `kms:Decrypt` if a CMK is used
- **Separate write role** (opt-in per state machine): `states:RedriveExecution`, `states:StartExecution`.
- **[U]** The exact resource types for ListMapRuns and DescribeMapRun, and the logs ARN formats. The Service Authorization Reference page did not render.

## 9. Console deep links

- **[U] Not documented by AWS.** The commonly used pattern is `https://<region>.console.aws.amazon.com/states/home?region=<region>#/v2/executions/details/<executionArn>`, and `#/statemachines/view/<stateMachineArn>` for a state machine.
- **[V]** The console does have Execution Details and Map Run Details pages (https://docs.aws.amazon.com/step-functions/latest/dg/concepts-view-execution-details.html).
- Make these links configurable templates.

## 10. Testing tools

**moto** (https://docs.getmoto.org/en/latest/docs/services/stepfunctions.html)
- **[V] Implemented:** describe_execution, get_execution_history, list_executions, describe_map_run, list_map_runs, describe_state_machine_for_execution, start/stop_execution, and others.
- **[V] Not implemented:** redrive_execution, test_state, validate_state_machine_definition, start_sync_execution.
- **[V] Behaviour.** By default executions do nothing and return static data. `SF_EXECUTION_HISTORY_TYPE=FAILURE` makes the static data report FAILED. `@mock_aws(config={"stepfunctions": {"execute_state_machine": True}})` turns on parsing and execution.

**Step Functions Local** (https://docs.aws.amazon.com/step-functions/latest/dg/sfn-local.html)
- **[V]** "Unsupported", with no feature parity. AWS recommends the TestState API instead. (Docker image `amazon/aws-stepfunctions-local`, port 8083.)

**TestState** (https://docs.aws.amazon.com/step-functions/latest/apireference/API_TestState.html)
- **[V]** Runs one state against real AWS. `inspectionLevel` is INFO, DEBUG or TRACE.
- **[V]** Accepts a `mock` (`result` or `errorOutput{error,cause}`) for Task, Map and Parallel states, plus `stateConfiguration` (`retrierRetryCount`, `errorCausedByState`, `mapIterationFailureCount`), `context` and `variables`.
- **[V]** Status is `SUCCEEDED | FAILED | RETRIABLE | CAUGHT_ERROR`. It runs for at most 5 minutes. Rate limit is 50/10.
- **[I]** This is useful for checking an ASL fix in CI.

## 11. Dagster and Prefect (short)

**Dagster**
- **[V] Failure sensor.** `@run_failure_sensor` receives a `RunFailureSensorContext` with `dagster_run.job_name`, `failure_event.message`, and `get_step_failure_events()`. Each step event gives `event.step_key` and `event.event_specific_data.error.to_string()`, which includes the stack (https://docs.dagster.io/api/dagster/schedules-sensors, https://docs.dagster.io/guides/automate/sensors/testing-run-status-sensors).
- **[V] GraphQL** (https://docs.dagster.io/api/graphql):
  - `runsOrError(filter:{statuses:[FAILURE]}, cursor, limit)`
  - `logsForRun(runId, limit, afterCursor)`, which returns `MessageEvent{stepKey, eventType, message, level}` and `LogsCapturedEvent{logKey, stepKeys}`
  - `capturedLogsMetadata(logKey)`, which returns stdout/stderr download URLs
  - Python client `DagsterGraphQLClient.get_run_status` (https://docs.dagster.io/api/graphql/dagster-graphql)
- **[V] Re-execution.** `ReexecutionOptions.from_failure(run_id, instance)` (https://docs.dagster.io/api/dagster/execution). The run-retry strategy is `FROM_FAILURE` (default) or `ALL_STEPS` (https://docs.dagster.io/deployment/run-retries).
- **[U]** The step-key format for dynamic or mapped ops.

**Prefect**
- **[V] Events.** For example `prefect.flow-run.Running`. The resource carries `prefect.resource.id` = `prefect.flow-run.<uuid>`, `prefect.state-type`, `prefect.state-message` and `prefect.run-count`. Related resources are the flow, deployment, work pool and tags (https://docs.prefect.io/v3/api-ref/events/flow-run-events, https://docs.prefect.io/v3/concepts/event-triggers).
- **[V] Automations.** `EventTrigger(expect={"prefect.flow-run.Failed"}, ...)` with actions such as `call-webhook`. Jinja templating can use `{{ flow_run.id }}`, `{{ flow_run.state.message }}` and `flow_run|ui_url` (https://docs.prefect.io/v3/automate/events/automations-triggers).
- **[V] REST.** `POST /api/flow_runs/filter` (filter on `state.type.any_ ["FAILED"]`; response has `state.message`, `run_count`, `empirical_policy`) and `POST /api/task_runs/filter` (filter by `flow_run_id`) (https://docs.prefect.io/v3/api-ref/rest-api/server/flow-runs/read-flow-runs, https://docs.prefect.io/v3/api-ref/rest-api/server/task-runs/read-task-runs).
- **[V] Retry.** `prefect flow-run retry` keeps the run ID and increments `run_count` (https://docs.prefect.io/v3/how-to-guides/workflows/retry-flow-runs).
- **[U]** The `/api/logs/filter` request shape.

---

## Implications for dag-healer's abstraction

These are concrete seams against the module interfaces of the current, Airflow-only implementation.

### 1. Incident identity

Today the key is `(dag_id, run_id, task_id, map_index:int, try_number:int)`. Generalize it to `(orchestrator, pipeline_id, run_id, task_id, map_key:str, attempt_key:str)`, plus a `native_ref` JSON column. Airflow maps one-to-one: `map_key = str(map_index)`, `attempt_key = str(try_number)`.

For Step Functions:

| Field | Value |
|---|---|
| `pipeline_id` | State machine name. Names are unique per account and region, so keep account and region in a namespace. |
| `run_id` | Execution name for Standard. For Express, use the ARN plus start date, because names are reusable. |
| `task_id` | State name. Unique in the whole definition, per the ASL spec. |
| `map_key` | Inline Map: `"<Map>[<index>]"`. Distributed Map: `"<label>/<child execution name>"`. |
| `attempt_key` | `"redrive<n>.retry<m>"`. Retry counts reset on redrive, and a Map retry creates a new Map Run. |

Keep the `dag_id` column name only as a compatibility alias.

### 2. Normalized FailureEvent (replaces the Airflow-shaped `ingest.build_payload`)

Fields: `orchestrator`, the ids above, `error_type`, `error_message`, `cause_raw` (masked), `occurred_at`, `terminal: bool`, `caught: bool`, `native: dict`.

The dedupe key must cope with best-effort, out-of-order and duplicate delivery. For Step Functions use `executionArn + redriveCount + failing state + event id`.

### 3. `OrchestratorAdapter` protocol (AirflowClient becomes one implementation)

Methods:
- `list_failures(since)`
- `parse_push(payload)`
- `get_attempt(ref)`
- `get_logs(ref) -> LogBundle`
- `get_definition(ref)`
- `retry(ref, mode)`
- `rerun(ref)`
- `links(ref)`
- `capabilities`

Capabilities flags:
- `task_retry`: Airflow clear versus Step Functions redrive (Standard only, ≤14 days, same definition)
- `full_rerun`: trigger, or StartExecution with the original input (≤256 KiB)
- `history_api`: false for Express
- `push_detection`
- `catches_visible`
- `ui_embed`: true only for Airflow
- `deploy_model`: `"git-sync"` or `"ci-deploy"`

### 4. Detection modes per adapter

**Step Functions**
- **Push:** EventBridge rule on FAILED/TIMED_OUT/ABORTED, delivered to either:
  - SQS polled by the worker (no inbound port needed; recommended), or
  - an API destination to `/events` (public HTTPS, 5-second response limit — fits the existing 202 + PendingEvent design).
- **Poll:** `ListExecutions(statusFilter=FAILED)` per state machine. It is eventually consistent and limited to 200/5.
- **Express and caught failures:** only via a CloudWatch Logs subscription or Logs Insights at level ERROR. These must be an explicit opt-in.
- **Terminal attempts only.** Raise an incident only for the final attempt (no further TaskScheduled). This matches Airflow, where a task with retries left goes to `up_for_retry` rather than `failed`.

### 5. Log resolution needs a `LogLocator` registry keyed by task resource type

| Resource type | How to find the logs |
|---|---|
| Lambda | Function log group from GetFunction `LoggingConfig`; time window from TaskStarted to TaskFailed; optional execution-id filter. |
| Batch | `DescribeJobs` → `logStreamName` |
| ECS | Task ARN → `prefix/container/task-id` stream |
| Glue | `JobRunId-driver` stream |

`get_task_log` becomes `adapter.get_logs(ref)`, returning a list of masked text blocks.

`agent/context.extract_traceback` must also parse Lambda's JSON `stackTrace` (lists of `[file, line, fn, code]`). `map_to_repo_path` must map `/var/task/...` to the resolved `CodeUri`.

### 6. `SourceResolver` registry (replaces the direct `repo/dagscan.scan_dags` dependency)

- **Airflow:** AST scanner (existing code).
- **Step Functions, in order:**
  1. ASL from DescribeStateMachineForExecution.
  2. Task → resource ARN (unwrap `${...}` DefinitionSubstitutions and `FunctionName`).
  3. IaC resolvers: SAM `CodeUri`+`Handler`; CDK `aws:asset:path` (warn that staged copies need hash matching back to source); Terraform `handler` + `archive_file.source_dir` (needs an HCL parser); Serverless `functions.<n>.handler`.
  4. Fallback: CloudFormation tags (logical-id / stack-name).
- Repair scope can then include `*.asl.json` and IaC files. Errors like `States.Runtime` (JSONPath) and `States.DataLimitExceeded` are definition or code bugs, not Lambda bugs.

### 7. Policy error taxonomy must be adapter-supplied

`agent/policy` currently hard-codes infra patterns. For Step Functions:
- **Infra:** `States.Timeout`, `States.HeartbeatTimeout`, `States.Permissions`, `Lambda.ServiceException`, `Lambda.SdkClientException`, `Lambda.AWSLambdaException`, `Lambda.TooManyRequestsException`, `Sandbox.Timedout`, `ExecutionLimitExceeded`, `States.Http.Socket`.
- **Definition/code:** `States.Runtime`, `States.DataLimitExceeded`.
- **Data/aggregate signals:** `States.ExceedToleratedFailureThreshold`, `States.ItemReaderFailed`.
- **Everything else:** the function's own `errorType` (e.g. `KeyError`), which goes through the existing data-shape gates.

Dataset locators need an `s3://` type, because Distributed Map ItemReader sources are CSV/JSON/JSONL/Parquet in S3.

### 8. "Repaired" is not "retryable" for Step Functions

Merging a PR does not deploy anything; deploys happen through CI. Redrive reuses the old definition, and only picks up new Lambda code if the Task points at an unqualified function ARN or a moved alias.

The `retry_task` action should therefore be gated on a `deployed_fix_check` capability, for example comparing GetFunction `CodeSha256`/`Version` against the commit. When the ASL changed, it should switch to StartExecution with the original input.

### 9. UI and authorization seams

- **`AccessControl` protocol.** `authorized_pipeline_ids(user, method)` and `can_edit(user, pipeline_id)` generalize `api/auth.py`. Airflow implements them with the auth manager. Standalone mode uses a healer.yaml mapping from users/groups to state machine ARN patterns.
- **Standalone UI.** Non-Airflow adapters need the same React views served by a standalone healer FastAPI with its own login. The views' host context (dagId/runId/taskId/mapIndex) should become `{orchestrator, pipelineId, runId, taskId, mapKey}`.
- **Links.** `links.py` becomes `adapter.links(ref)`, with configurable console templates (section 9).

### 10. AWS credentials

- Two roles: read (default) and write (opt-in redrive/start), scoped per state machine with `aws:ResourceTag` conditions.
- Use `includeExecutionData=false` by default to avoid pulling PII payloads.
- Still mask `cause` (up to 32 KB) before persisting it.

### 11. Tests for the reference Step Functions adapter

- Recorded history/redrive JSON fixtures driven through botocore's Stubber (a design choice; not verified).
- moto for list/describe calls (static history unless `execute_state_machine` is on).
- No moto support for `redrive_execution` or `test_state`.
- An optional TestState-based check in CI.
- Do not use Step Functions Local (unsupported).

### 12. Dagster and Prefect fit the same seam

Both are Python-source orchestrators, so the AST source resolver and pytest repair can be reused.

| Concept | Dagster | Prefect |
|---|---|---|
| Pipeline | job | flow/deployment |
| Run | run_id | flow_run UUID |
| Task | step_key | task run |
| Map key | dynamic mapping key (a string) | — |
| Attempt | step attempt | `run_count` |
| Detection | `@run_failure_sensor` (in-process, like the Airflow listener) or GraphQL polling | automation `call-webhook` to `/events`, or REST `/flow_runs/filter` polling |
| Logs | `logsForRun` + `capturedLogsMetadata` | — |
| Retry | `ReexecutionOptions.from_failure` / `FROM_FAILURE` | `flow-run retry` |

Both confirm the two key generalizations: `map_key` must be a string, and `attempt_key` must be an opaque string.

---

**Unverified items to check later:**
- The console URL formats.
- Whether a TaskFailed `cause` includes the Lambda `requestId`.
- The CloudWatch record fields `previous_event_id` and `redrive_count`.
- The resource types for ListMapRuns/DescribeMapRun.
- Whether Distributed Map Standard children emit EventBridge events.
- The Express execution ARN format.
- The `.sync` cause content for ECS, Batch and Glue.
- EventBridge connection auth types.
