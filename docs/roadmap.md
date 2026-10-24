# Roadmap

dag-healer reinforces existing pipelines. When a task fails it decides whether the input data or the code is at fault, alerts the people who can fix the data, and proposes a tested fix for the code. This page lists where the project is going and in which order. The order changes as pilots teach us more.

The steps of the orchestrator work are tracked in section 12.1 of [docs/architecture/orchestrators.md](architecture/orchestrators.md).

## Principles

- **Fixed rules decide, the model helps.** A DATA verdict needs a violated, approved contract. A CODE verdict is refused while an approved contract is violated. Infrastructure failures go to a person. The model works within these rules.
- **Nothing changes without review.** Fixes, contracts and context change through pull requests. dag-healer never merges, and never writes to production databases.
- **An incident is done when the pipeline runs again**, not when a pull request or an alert exists.
- **One incident per cause, not per failure.** On-call people stop using a tool that is noisy.

## Done recently

- `dag-healer serve`: the standalone console and API, for orchestrators without a UI of their own. Sign-in with tokens, a trusted proxy or an AWS load balancer, and permissions per pipeline.
- The AWS Step Functions adapter, with an example repository that deploys with AWS SAM ([examples/stepfunctions_repo](../examples/stepfunctions_repo/README.md)).
- "Retry task" also re-runs the tasks after the failed one that failed or were blocked by it.
- Model-written tests and fixes run in a container sandbox without network, credentials or host paths (G1). Stronger backends, such as ECS/Fargate tasks or Kubernetes Jobs, can come later.

## Now

- Security hardening before the first pilots (see [docs/security.md](security.md)):
  - keep CI secrets away from `healer/` branches until a person reviews them (G2);
  - enforce the branch rules on GitHub instead of only in dag-healer, and use a token limited to one repository (G3).
- Documentation for both orchestrators (step 9 of section 12.1): writing an adapter, deploying on AWS, and upgrading the store.
- A test of the Step Functions adapter in a real AWS account, with the example repository.

## Next: reliable on-call

1. **Incidents last until the pipeline recovers.**
   - New statuses: Fix ready, Retrying, Resolved, Waiting for data, and "failed again".
   - dag-healer follows the result of every retry.
   - A person marks a fix as deployed. Detecting merges and deploys automatically comes later.
2. **A snapshot of the input at failure time.**
   - The contract is checked against what the failed run read: the file, its size, modification time, row count, columns and a masked sample.
   - If the input changed after the failure, dag-healer does not decide: the incident goes to a person, with that reason.
   - Per-run locators come later: dated paths and partitions.
3. **Less noise.**
   - Repeats of a failure fold into the open incident.
   - A failure can be muted for a while, with a reason.
   - The triage queue is visible.
   - Later: a storm mode that sends one summary instead of many alerts, and groups of pipelines that failed for the same cause, with one decision for all.
4. **Infrastructure errors are recognized as such.** These include expired credentials, missing permissions, throttling and quotas, for example HTTP 401, 403 and 429. dag-healer suggests the action ("rotate the token", "retry in 15 minutes") instead of changing code.
5. **Shadow mode, set per pipeline.**
   - dag-healer classifies and proposes fixes with no side effects: no alerts to other teams, no pull requests, no retries.
   - The decision log compares what it would have done with what on-call decided.
   - Each pipeline moves to active mode once the two agree.
6. **Contract approval through a pull request.** When a person decides that the input changed on purpose, the contract change and the code fix go in the same pull request.
7. **Alerts reach the people who can act.**
   - Owners are listed per pipeline.
   - A producer team inside the organization gets a direct message in its own terms.
   - An external producer, such as a client or a vendor, is never contacted. On-call and the person who owns that relationship get a draft to forward instead.
   - Email and Slack come first.

## Next: context

- **Organization Context and ETL Information.**
  - Two kinds of Markdown file in the repository:
    - `healer/organization.md` holds what the whole organization knows: what the business does, its systems and their owners, a glossary, and the rules for every pipeline.
    - `healer/etl/<pipeline>.md` holds what one pipeline needs.
  - Each file has a front matter with its owner and review date, and its rules have stable IDs such as `ORG-1` or `SALES-1`.
  - dag-healer reads them from the default branch, refuses files that contain secrets, and caps their size.
- **Cited, never obeyed blindly.**
  - Incidents and pull requests list the rules applied, and dag-healer checks that every cited ID exists.
  - Context can make a verdict more cautious, but it never overrides the fixed rules.
  - A fix that would break a rule stops and asks a person.
- **Suggestions, not edits.**
  - dag-healer suggests additions: from a person's decision for that pipeline, and from patterns across pipelines for the Organization Context.
  - A person merges each suggestion.
  - dag-healer never edits these files.
- **Decision log.**
  - Every decision is recorded, with who made it and why: a fixed rule, the model with its confidence, or a person.
  - Planned: a daily summary posted to Slack or email.
- **The console leads with the decision.**
  - A waiting incident opens with the question in plain words, and shows the consequences before you confirm.
  - A queued action can be cancelled.
  - Every incident says what decided it.

## Later: more sources

1. **SQL sources** through Trino, and PostgreSQL and MySQL/MariaDB directly.
   - Contracts on tables: the schema and a masked sample, read with a read-only connection.
   - Schema changes are detected before the pipeline runs.
2. **Stored procedures** (PostgreSQL, MySQL).
   - Database errors are classified, and the incident shows the procedure's code.
   - A procedure versioned in the repository gets a pull request with a test (pgTAP, or a scratch database).
   - For an unversioned procedure, dag-healer proposes a script for a person to review and apply.
   - `dag-healer init` can export procedures into the repository.
3. **API sources.** Contracts on responses (fields and types), and a snapshot of the response at failure time. Credentials are never stored.
4. **dbt.** First, dbt tests are read as contract rules and dbt failures are classified. Repairing SQL models comes later.
5. **Other sources:** warehouses beyond Trino (Snowflake, BigQuery, Redshift), partitioned datasets, and event payloads such as a Step Functions execution input.

## Not planned

- Merging pull requests or changing production databases.
- Creating new pipelines: dag-healer reinforces the ones you have.
