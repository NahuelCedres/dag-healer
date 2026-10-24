#!/usr/bin/env bash
# Start the three executions of one day on the deployed sfn-etl stack (AWS CLI v2).
#
#   scripts/start_executions.sh [RUN_DATE]        RUN_DATE: YYYY-MM-DD, default today (UTC)
#
# Environment: STACK_NAME (default sfn-etl), AWS_REGION (default eu-west-1), REQUESTED_BY (who
# asks for the customers sync, default data-team@example.com) and NAME_SUFFIX (appended to the
# execution names: an execution name can be used once per state machine, so set it to run the
# same day again). Prints the ARN of each execution it starts.
set -euo pipefail

STACK_NAME="${STACK_NAME:-sfn-etl}"
REGION="${AWS_REGION:-eu-west-1}"
RUN_DATE="${1:-$(date -u +%Y-%m-%d)}"
REQUESTED_BY="${REQUESTED_BY:-data-team@example.com}"
NAME_SUFFIX="${NAME_SUFFIX:-}"

if [[ ! "$RUN_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "usage: $0 [YYYY-MM-DD]" >&2
  exit 2
fi

output() {
  aws cloudformation describe-stacks --region "$REGION" --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}

start() {
  aws stepfunctions start-execution --region "$REGION" \
    --state-machine-arn "$1" --name "$2$NAME_SUFFIX" --input "$3" \
    --query executionArn --output text
}

start "$(output SalesDailyEtlStateMachineArn)" "sales-$RUN_DATE" \
  "{\"run_date\": \"$RUN_DATE\"}"
start "$(output CustomersSyncStateMachineArn)" "customers-$RUN_DATE" \
  "{\"run_date\": \"$RUN_DATE\", \"requested_by\": \"$REQUESTED_BY\"}"
start "$(output InventoryReportStateMachineArn)" "inventory-$RUN_DATE" \
  "{\"run_date\": \"$RUN_DATE\", \"warehouses\": [\"Reno\", \"Dallas\", \"Newark\"]}"
