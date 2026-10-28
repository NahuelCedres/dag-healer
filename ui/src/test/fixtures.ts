/**
 * API payloads used by the console tests (shapes of the /healer/api/v1 interface), modelled on what
 * the demo records: `inventory_report` waiting for a decision (a draft contract), `sales_daily_etl`
 * reported as a data problem, `customers_sync` with a proposed fix and its PR.md.
 */
import type {
  ContractView,
  DagSummary,
  IncidentDetail,
  IncidentEvent,
  IncidentSummary,
  SetupResponse,
} from "src/api";

export const RUN_ID = "manual__2026-10-17T12:00:00+00:00";
export const AT = "2026-10-17T12:05:00+00:00";

export const summary = (overrides: Partial<IncidentSummary> = {}): IncidentSummary => ({
  category: "code",
  confidence: 0.9,
  created_at: AT,
  dag_id: "customers_sync",
  error_summary: "ValueError: unconverted data remains: T08:30:00Z",
  has_pr_md: false,
  id: "a1b2c3d4e5f60718293a4b5c6d7e8f90",
  map_index: -1,
  occurrences: 1,
  pr_url: null,
  run_id: RUN_ID,
  source: "rest",
  status: "repaired",
  task_id: "transform",
  try_number: 1,
  updated_at: "2026-10-17T12:09:00+00:00",
  ...overrides,
});

export const event = (
  kind: string,
  message: string,
  data: unknown = null,
  actor = "dag-healer",
): IncidentEvent => ({
  actor,
  at: AT,
  data,
  kind,
  message,
});

const classification = (data: Record<string, unknown>): IncidentEvent =>
  event("classification", `Classified as ${String(data.category)} (${String(data.rule)})`, {
    decided_by: "policy",
    llm_category: null,
    llm_confidence: null,
    needs_llm_confirmation: false,
    violations: [],
    ...data,
  });

const context = (data: Record<string, unknown>): IncidentEvent =>
  event("context", "Context gathered", {
    datasets_checked: [],
    draft_violation_count: 0,
    notes: [],
    repo_frames: [],
    violation_count: 0,
    ...data,
  });

const baseDetail = (overrides: Partial<IncidentDetail> = {}): IncidentDetail => ({
  ...summary(),
  alerts: [],
  allowed_actions: [],
  attempts: 0,
  can_edit: true,
  diagnosis: null,
  diff: null,
  events: [],
  evidence: [],
  links: { dag: null, healer: null, task_instance: null },
  pending_commands: [],
  pr_md_url: null,
  pytest_output: null,
  resolution: null,
  resolved_by: null,
  sample: null,
  test_code: null,
  test_path: null,
  tokens_used: 0,
  violated_rule: null,
  violations: [],
  ...overrides,
});

const MISSING_QTY = {
  actual: ["quantity", "sku", "updated_at", "warehouse", "warehouse_code"],
  column: "qty",
  dataset: "inventory",
  expected: "qty",
  message: "Column 'qty' is missing from dataset 'inventory'.",
  row_indexes: [],
  rule: "missing_column",
};

/** AMBIGUOUS, waiting for a person: the draft contract expects `qty`, the file has `quantity`. */
export const ambiguousIncident = (overrides: Partial<IncidentDetail> = {}): IncidentDetail =>
  baseDetail({
    allowed_actions: ["resolve_data", "resolve_code", "retry_task", "ignore"],
    category: "ambiguous",
    confidence: 0.5,
    dag_id: "inventory_report",
    diagnosis: "KeyError on column qty, but the contract is still a draft.",
    error_summary: "KeyError: 'qty'",
    events: [
      event("created", "Incident created from rest for inventory_report.build_report"),
      context({
        contract_status: "draft",
        datasets_checked: ["inventory"],
        draft_violation_count: 1,
        exception_message: "'qty'",
        exception_type: "KeyError",
        repo_frames: [{ file: "src/etl/inventory.py", function: "buildReport", line: 28 }],
        sample_dataset: "inventory",
      }),
      classification({
        category: "ambiguous",
        confidence: 0.5,
        contract_status: "draft",
        reason:
          "There is no approved data contract (status: draft) and the task failed with a data-shaped error.",
        rule: "c_no_approved_contract",
        violations: [{ ...MISSING_QTY, contract_status: "draft" }],
      }),
      event("status", "Waiting for a human decision", { from: "classified", to: "waiting_human" }),
    ],
    id: "ambiambiambiambiambiambiambiambi",
    sample: {
      columns: ["sku", "warehouse", "warehouse_code", "quantity", "updated_at"],
      dataset: "inventory",
      masked: true,
      rows: [
        {
          quantity: 128,
          sku: "SKU-1001",
          updated_at: "2026-10-02",
          warehouse: "Reno",
          warehouse_code: "RNO-1",
        },
        {
          quantity: null,
          sku: "SKU-1002",
          updated_at: "2026-10-02",
          warehouse: "Dallas",
          warehouse_code: "DFW-2",
        },
      ],
    },
    status: "waiting_human",
    task_id: "build_report",
    violated_rule: { ...MISSING_QTY, contract_status: "draft" },
    violations: [MISSING_QTY],
    ...overrides,
  });

/** DATA, reported: the approved contract of sales_daily_etl lost its `amount` column. */
export const dataIncident = (overrides: Partial<IncidentDetail> = {}): IncidentDetail =>
  baseDetail({
    alerts: [
      {
        channel: "email",
        error: null,
        recipients: ["data-team@example.com"],
        sent_at: AT,
        status: "sent",
        subject: "[dag-healer] DATA issue in sales_daily_etl.transform_sales",
      },
    ],
    allowed_actions: ["resolve_code", "retry_task", "ignore"],
    category: "data",
    confidence: 0.95,
    dag_id: "sales_daily_etl",
    diagnosis: "The sales file renamed column amount to amt.",
    error_summary: "KeyError: 'amount'",
    events: [
      context({ contract_status: "approved", datasets_checked: ["sales"], violation_count: 1 }),
      classification({
        category: "data",
        confidence: 0.95,
        contract_status: "approved",
        reason: "The approved contract is violated (missing_column: sales.amount).",
        rule: "a_contract_violated",
        violations: [{ column: "amount", dataset: "sales", rule: "missing_column" }],
      }),
    ],
    id: "datadatadatadatadatadatadatadata",
    sample: {
      columns: ["order_id", "customer_email", "amt"],
      dataset: "sales",
      masked: true,
      rows: [
        { amt: 10.5, customer_email: "h:3f2a9c1b07", order_id: 1 },
        { amt: null, customer_email: "h:77aa01de55", order_id: 2 },
      ],
    },
    status: "reported",
    task_id: "transform_sales",
    violated_rule: {
      column: "amount",
      dataset: "sales",
      message: "Column 'amount' is missing",
      rule: "missing_column",
    },
    violations: [
      {
        actual: ["amt", "customer_email", "order_id"],
        column: "amount",
        dataset: "sales",
        expected: "amount",
        message: "Column 'amount' is missing",
        row_indexes: [],
        rule: "missing_column",
      },
      {
        actual: { null_count: 1, rows: 2 },
        column: "amt",
        dataset: "sales",
        expected: { nullable: false },
        message: "1 null value",
        row_indexes: [1],
        rule: "nulls_not_allowed",
      },
    ],
    ...overrides,
  });

export const DIFF = [
  "diff --git a/src/etl/customers.py b/src/etl/customers.py",
  "index 1111111..2222222 100644",
  "--- a/src/etl/customers.py",
  "+++ b/src/etl/customers.py",
  "@@ -10,3 +10,3 @@ def parse_signup_date(value):",
  "     value = value.strip()",
  '-    return datetime.strptime(value, "%Y-%m-%d")',
  '+    return datetime.fromisoformat(value.replace("Z", "+00:00"))',
  "     # end",
  "diff --git a/tests/test_customers_signup_date.py b/tests/test_customers_signup_date.py",
  "new file mode 100644",
  "--- /dev/null",
  "+++ b/tests/test_customers_signup_date.py",
  "@@ -0,0 +1,2 @@",
  "+def test_parse_signup_date_accepts_iso_timestamps():",
  '+    assert parse_signup_date("2026-10-02T14:03:00Z").year == 2026',
  "",
].join("\n");

export const PYTEST_OUTPUT = [
  "=== pytest before the fix (the new test must fail) ===",
  "$ pytest tests/test_customers_signup_date.py",
  "exit code 1: 1 passed, 1 failed, 0 errors, 0 skipped, 2 collected in 0.1s",
  "E           ValueError: unconverted data remains: T14:03:00Z",
  "",
  "=== pytest after the fix ===",
  "$ pytest tests/test_customers_signup_date.py",
  "exit code 0: 2 passed, 0 failed, 0 errors, 0 skipped, 2 collected in 0.1s",
].join("\n");

export const PR_MD = [
  "# dag-healer fix: `transform` in `customers_sync`",
  "",
  "## Test results",
  "",
  "2 passed",
  "",
  "## CI",
  "",
  "CI has not run for this change. Its commit carries \\[skip ci\\], so GitHub starts no workflow for it.",
  "",
  "After your review, start CI with an empty commit:",
  "",
  "```sh",
  "git push --no-verify origin healer/customers_sync-transform-a1b2c3d4",
  "```",
  "",
  "## Checklist",
  "",
  "- [ ] Review",
].join("\n");

/** CODE, a fix proposed as PR.md (no GitHub token). */
export const codeIncident = (overrides: Partial<IncidentDetail> = {}): IncidentDetail =>
  baseDetail({
    allowed_actions: ["retry_task"],
    attempts: 2,
    diagnosis: "parse_signup_date only accepts dates; the contract allows ISO timestamps.",
    diff: DIFF,
    events: [
      context({
        contract_status: "approved",
        datasets_checked: ["customers"],
        exception_message: "unconverted data remains: T08:30:00Z",
        exception_type: "ValueError",
        repo_frames: [{ file: "src/etl/customers.py", function: "parse_signup_date", line: 19 }],
      }),
      classification({
        category: "code",
        confidence: 0.9,
        contract_status: "approved",
        decided_by: "anthropic",
        llm_category: "code",
        llm_confidence: 0.9,
        needs_llm_confirmation: true,
        reason: "The data satisfies the approved contract and the LLM confirmed a code defect.",
        rule: "b_code_with_valid_data",
      }),
      event("repair", "Fix proposed on branch healer/customers_sync-transform-a1b2c3d4", {
        branch: "healer/customers_sync-transform-a1b2c3d4",
        notes: ["Branch `healer/customers_sync-transform-a1b2c3d4` was pushed to the local repository."],
        pr_md_path: "/opt/airflow/healer/prs/1/PR.md",
        status: "repaired",
        test_path: "tests/test_customers_signup_date.py",
      }),
    ],
    has_pr_md: true,
    pr_md_url: "http://localhost:8080/healer/api/v1/dags/customers_sync/incidents/x/pr.md",
    pytest_output: PYTEST_OUTPUT,
    status: "repaired",
    test_code: "def test_parse_signup_date_accepts_iso_timestamps():\n    assert True\n",
    test_path: "tests/test_customers_signup_date.py",
    tokens_used: 12_345,
    ...overrides,
  });

export const draftContract = (overrides: Partial<ContractView> = {}): ContractView => ({
  approved_at: null,
  approved_by: null,
  can_approve: true,
  dag_id: "inventory_report",
  datasets: [
    {
      columns: [
        {
          allowed_values: null,
          dtype: "string",
          max: null,
          min: null,
          name: "sku",
          note: null,
          nullable: false,
          pii: false,
        },
        {
          allowed_values: null,
          dtype: "integer",
          max: 3100,
          min: 0,
          name: "qty",
          note: null,
          nullable: false,
          pii: false,
        },
        {
          allowed_values: null,
          dtype: "string",
          max: null,
          min: null,
          name: "warehouse",
          note: null,
          nullable: false,
          pii: false,
        },
        {
          allowed_values: null,
          dtype: "datetime",
          max: null,
          min: null,
          name: "updated_at",
          note: null,
          nullable: false,
          pii: false,
        },
      ],
      locator: { path: "data/incoming/inventory.csv", type: "csv" },
      name: "inventory",
      row_count: { max: 100, min: 15 },
      task_id: "get_inventory",
    },
  ],
  path: "healer/contracts/inventory_report.yaml",
  pending_approval: false,
  stale: false,
  status: "draft",
  ...overrides,
});

export const DRAFT_YAML = [
  "# Draft contract generated by dag-healer init. Review it and approve it before it is enforced.",
  "version: 1",
  "dag_id: inventory_report",
  "status: draft",
  "datasets:",
  "  - name: inventory",
  "    task_id: get_inventory",
  "    locator:",
  "      type: csv",
  "      path: data/incoming/inventory.csv",
  "    row_count:",
  "      min: 15",
  "      max: 100",
  "    allow_extra_columns: true",
  "    columns:",
  "      - name: sku",
  "        dtype: string",
  "        nullable: false",
  "      - name: qty",
  "        dtype: integer",
  "        nullable: false",
  "        min: 0.0",
  "        max: 3100.0",
  "      - name: updated_at",
  "        dtype: datetime",
  "        nullable: false",
  "",
].join("\n");

export const approvedContract = (overrides: Partial<ContractView> = {}): ContractView =>
  draftContract({
    approved_at: "2026-10-16T09:00:00+00:00",
    approved_by: "admin",
    can_approve: true,
    dag_id: "sales_daily_etl",
    path: "healer/contracts/sales_daily_etl.yaml",
    status: "approved",
    ...overrides,
  });

export const dagSummary = (overrides: Partial<DagSummary> = {}): DagSummary => ({
  contract_status: "approved",
  dag_id: "sales_daily_etl",
  last_incident: null,
  monitored: true,
  open_incidents: 0,
  ...overrides,
});

export const setup = (overrides: Partial<SetupResponse> = {}): SetupResponse => ({
  alerts: {
    email: ["data-team@example.com"],
    events: ["waiting_human", "reported", "repaired", "repair_failed"],
  },
  contracts_dir: "healer/contracts",
  min_confidence: 0.75,
  repairs: {
    allowed_paths: ["src/", "dags/"],
    base_branch: "main",
    max_diff_files: 10,
    max_diff_lines: 400,
    pr_base: "main",
    repository: null,
    skip_ci: true,
    tests_dir: null,
    token_configured: false,
  },
  ...overrides,
});
