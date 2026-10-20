/** API payloads used by the view tests (shapes of the /healer/api/v1 interface). */
import type { ContractView, DagSummary, IncidentDetail, IncidentSummary } from "src/api";

export const RUN_ID = "manual__2026-10-17T12:00:00+00:00";

export const summary = (overrides: Partial<IncidentSummary> = {}): IncidentSummary => ({
  category: "code",
  confidence: 0.9,
  created_at: "2026-10-17T12:05:00+00:00",
  dag_id: "customers_sync",
  error_summary: "ValueError: time data '2026-10-02T14:03:00Z' does not match format '%Y-%m-%d'",
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
  "",
].join("\n");

export const TEST_CODE = [
  "from etl.customers import parse_signup_date",
  "",
  "",
  "def test_parse_signup_date_accepts_iso_timestamps():",
  '    assert parse_signup_date("2026-10-02T14:03:00Z").year == 2026',
  "",
].join("\n");

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

/** CODE incident repaired with a PR.md (no GitHub token). */
export const codeIncident = (overrides: Partial<IncidentDetail> = {}): IncidentDetail =>
  baseDetail({
    alerts: [
      {
        channel: "email",
        error: null,
        recipients: ["data-team@example.com"],
        sent_at: "2026-10-17T12:09:30+00:00",
        status: "sent",
        subject: "[dag-healer] customers_sync.transform repaired",
      },
    ],
    allowed_actions: ["retry_task"],
    attempts: 1,
    diagnosis: "parse_signup_date only accepts dates; the contract allows ISO timestamps.",
    diff: DIFF,
    events: [
      {
        actor: "dag-healer",
        at: "2026-10-17T12:05:01+00:00",
        data: null,
        kind: "created",
        message: "Incident created",
      },
      {
        actor: "dag-healer",
        at: "2026-10-17T12:05:30+00:00",
        data: {
          category: "code",
          reason: "Data passes the approved contract",
          rule: "b_code_with_valid_data",
        },
        kind: "classified",
        message: "Classified as code",
      },
    ],
    evidence: ["customers.csv passes the approved contract", "Traceback ends in src/etl/customers.py:12"],
    has_pr_md: true,
    pr_md_url: "http://airflow-apiserver:8080/healer/api/v1/dags/customers_sync/incidents/x/pr.md",
    pytest_output: "tests/test_customers_parse_signup_date.py .\n1 passed in 0.12s",
    status: "repaired",
    test_code: TEST_CODE,
    test_path: "tests/test_customers_parse_signup_date.py",
    tokens_used: 12_345,
    ...overrides,
  });

/** DATA incident with a violated rule and a masked sample. */
export const dataIncident = (overrides: Partial<IncidentDetail> = {}): IncidentDetail =>
  baseDetail({
    allowed_actions: ["resolve_code", "retry_task", "ignore"],
    category: "data",
    confidence: 1,
    dag_id: "sales_daily_etl",
    diagnosis: "The sales file renamed column amount to amt.",
    error_summary: "KeyError: 'amount'",
    id: "datadatadatadatadatadatadatadata",
    sample: {
      columns: ["order_id", "customer_email", "amt"],
      dataset: "sales",
      masked: true,
      rows: [
        { amt: 10.5, customer_email: "h:3f2a9c1b", order_id: 1 },
        { amt: null, customer_email: "h:77aa01de", order_id: 2 },
      ],
    },
    status: "reported",
    task_id: "load_sales",
    violated_rule: {
      column: "amount",
      dataset: "sales",
      message: "Column 'amount' is missing",
      rule: "missing_column",
    },
    violations: [
      {
        column: "amount",
        dataset: "sales",
        message: "Column 'amount' is missing",
        row_indexes: [],
        rule: "missing_column",
      },
      {
        actual: 1,
        column: "amt",
        dataset: "sales",
        expected: 0,
        message: "1 null value",
        row_indexes: [1],
        rule: "nulls_not_allowed",
      },
    ],
    ...overrides,
  });

/** AMBIGUOUS incident waiting for a human. */
export const ambiguousIncident = (overrides: Partial<IncidentDetail> = {}): IncidentDetail =>
  baseDetail({
    allowed_actions: ["resolve_data", "resolve_code", "ignore"],
    category: "ambiguous",
    confidence: 0.5,
    dag_id: "inventory_report",
    diagnosis: "KeyError on column qty, but the contract is still a draft.",
    error_summary: "KeyError: \"['qty'] not in index\"",
    id: "ambiambiambiambiambiambiambiambi",
    status: "waiting_human",
    task_id: "build_report",
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
          max: 1000,
          min: 0,
          name: "qty",
          note: "inferred from 42 rows",
          nullable: false,
          pii: false,
        },
      ],
      locator: { path: "data/incoming/inventory.csv", type: "csv" },
      name: "inventory",
      row_count: { max: null, min: 1 },
      task_id: "build_report",
    },
  ],
  path: "healer/contracts/inventory_report.yaml",
  pending_approval: false,
  stale: false,
  status: "draft",
  ...overrides,
});

export const approvedContract = (overrides: Partial<ContractView> = {}): ContractView =>
  draftContract({
    approved_at: "2026-10-16T09:00:00+00:00",
    approved_by: "admin",
    dag_id: "sales_daily_etl",
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
