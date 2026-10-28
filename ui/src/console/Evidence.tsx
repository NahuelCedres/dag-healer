/**
 * The evidence of an incident: what happened, the contract check (each broken rule in plain words
 * next to the lines of the YAML file that hold it), the masked sample, the error and how dag-healer
 * decided.
 */
import { useId } from "react";

import type { ContractView, IncidentDetail, MaskedSample, Violation } from "src/api";
import { formatValue } from "src/format";
import { useUiText } from "src/vocabulary";

import { classificationOf, contextOf } from "./events";
import { dtypeWord, ruleWord, stepLabel } from "./labels";
import { NavLink } from "./nav";
import { type LineRange, yamlRuleRange } from "./texts";
import { RichText, toneStyle } from "./ui";

const asRecord = (value: unknown): Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};

const num = (value: unknown): string | undefined =>
  typeof value === "number" && Number.isFinite(value) ? value.toLocaleString("en-US") : undefined;

const quoteList = (values: ReadonlyArray<unknown>, max = 8): string => {
  const shown = values.slice(0, max).map((value) => `\`${String(value)}\``);

  return values.length > max ? `${shown.join(", ")} and ${values.length - max} more` : shown.join(", ");
};

/** One broken contract rule in plain words. */
export const violationSentence = (violation: Violation): string => {
  const column = violation.column ? `\`${violation.column}\`` : "the data";
  const expected = asRecord(violation.expected);
  const actual = asRecord(violation.actual);
  const rows = violation.row_indexes?.length ?? 0;

  switch (violation.rule) {
    case "missing_column": {
      const present = Array.isArray(violation.actual) ? violation.actual : [];

      return (
        `The contract expects a column ${column}; the file has none.` +
        (present.length > 0 ? ` Its columns are ${quoteList(present)}.` : "")
      );
    }
    case "unexpected_column":
      return `The file has a column ${column} that the contract does not list, and the contract accepts no other columns.`;
    case "nulls_not_allowed": {
      const count = num(actual.null_count);
      const total = num(actual.rows);

      return count === undefined
        ? `${column} must never be empty, and it is empty in some rows.`
        : `${column} must never be empty; it is empty in ${count}${total === undefined ? "" : ` of ${total}`} rows.`;
    }
    case "dtype_mismatch":
      return (
        `${column} must hold ${dtypeWord(String(violation.expected ?? "unknown"))}` +
        (typeof violation.actual === "string" ? `; the file holds ${dtypeWord(violation.actual)}` : "") +
        (rows > 0 ? ` (${rows} ${rows === 1 ? "value" : "values"} do not fit).` : ".")
      );
    case "out_of_range": {
      const min = num(expected.min);
      const max = num(expected.max);
      const bounds =
        min !== undefined && max !== undefined
          ? `between ${min} and ${max}`
          : min === undefined
            ? `at most ${max ?? "?"}`
            : `at least ${min}`;
      const seen =
        num(actual.min) !== undefined && num(actual.max) !== undefined
          ? ` (the file has values from ${num(actual.min)} to ${num(actual.max)})`
          : "";

      return `${column} must be ${bounds}; ${num(actual.count) ?? "some"} values are outside${seen}.`;
    }
    case "not_allowed_value": {
      const allowed = Array.isArray(expected.allowed_values) ? expected.allowed_values : [];
      const found = Array.isArray(actual.values) ? actual.values : [];

      return (
        `${column} must be one of ${quoteList(allowed)}; ${num(actual.count) ?? "some"} values are not` +
        (found.length > 0 ? ` (${quoteList(found)}).` : ".")
      );
    }
    case "row_count_out_of_band": {
      const min = num(expected.min) ?? "0";
      const max = num(expected.max);

      return `The file must have ${max === undefined ? `at least ${min}` : `between ${min} and ${max}`} rows; it has ${
        num(violation.actual) ?? "a different number"
      }.`;
    }
    case "dataset_unreadable":
      return `dag-healer could not read ${typeof violation.expected === "string" ? `\`${violation.expected}\`` : "the file"}: ${
        violation.message ?? "unknown error"
      }`;
    default:
      return violation.message ?? violation.rule;
  }
};

/** The YAML key that holds a dataset-level rule. */
const RULE_KEYS: Record<string, string> = {
  dataset_unreadable: "locator",
  row_count_out_of_band: "row_count",
  unexpected_column: "allow_extra_columns",
};

const YamlExcerpt = ({
  file,
  path,
  range,
}: {
  readonly file: string;
  readonly path: string;
  readonly range: LineRange;
}) => {
  const lines = file.split(/\r?\n/u).slice(range.first, range.last + 1);
  const indent = Math.min(
    ...lines.filter((line) => line.trim()).map((line) => line.length - line.trimStart().length),
  );

  return (
    <figure className="stack" style={{ gap: 4 }}>
      <figcaption className="faint small mono">
        {path}, line {range.first + 1}
        {range.last > range.first ? `–${range.last + 1}` : ""}
      </figcaption>
      <pre className="code" tabIndex={0}>
        {lines.map((line) => line.slice(Number.isFinite(indent) ? indent : 0)).join("\n")}
      </pre>
    </figure>
  );
};

const contractLead = (
  status: string | undefined,
  pipeline: string,
  failed: number,
  checked: boolean,
): string => {
  switch (status) {
    case "approved":
      if (failed > 0) {
        return `The input breaks the approved contract of \`${pipeline}\`: ${failed} ${failed === 1 ? "check" : "checks"} failed.`;
      }

      return checked
        ? `The input passes every check of the approved contract of \`${pipeline}\`.`
        : `The contract of \`${pipeline}\` is approved, but no input was checked against it.`;
    case "draft":
      return (
        `The contract of \`${pipeline}\` is a draft: nobody approved it yet, so what it finds is evidence only and ` +
        "dag-healer does not act on it."
      );
    case "stale":
      return (
        `The contract of \`${pipeline}\` changed after it was approved: dag-healer treats it as unapproved until ` +
        "someone approves it again."
      );
    default:
      return (
        `\`${pipeline}\` has no contract, so nothing was checked automatically. \`dag-healer init\` drafts one from ` +
        "a sample of the input."
      );
  }
};

export const ContractCheck = ({
  contract,
  file,
  incident,
}: {
  readonly contract: ContractView | null | undefined;
  readonly file?: string;
  readonly incident: IncidentDetail;
}) => {
  const id = useId();
  const context = contextOf(incident.events);
  const status =
    classificationOf(incident.events)?.contractStatus ??
    context?.contractStatus ??
    (contract === null ? "missing" : contract?.status);
  const violations = incident.violations;
  const checked = (context?.datasetsChecked.length ?? 0) > 0;
  const path = contract?.path ?? "the contract file";

  return (
    <section aria-labelledby={id} className="card" data-testid="contract-check">
      <div className="card-h">
        <h2 id={id}>The contract check</h2>
        {contract === undefined || contract === null ? undefined : (
          <NavLink className="btn sm ghost" place={{ pipeline: incident.dag_id, to: "pipelines" }}>
            Open the contract
          </NavLink>
        )}
      </div>
      <p className="lead">
        <RichText
          text={contractLead(status, incident.dag_id, status === "approved" ? violations.length : 0, checked)}
        />
      </p>
      {violations.length === 0 ? undefined : (
        <div className="rules">
          {violations.map((violation, index) => {
            const range =
              file === undefined
                ? undefined
                : yamlRuleRange(
                    file,
                    violation.dataset ?? contract?.datasets[0]?.name,
                    violation.rule === "unexpected_column" ? undefined : violation.column,
                    RULE_KEYS[violation.rule],
                  );

            return (
              // Violations are listed as recorded and never reordered.
              <div
                className="rule"
                data-rule={violation.rule}
                key={`${violation.rule}-${violation.column ?? ""}-${index}`}
              >
                <div className="plain">
                  <p className="row" style={{ gap: 8 }}>
                    <span className="pill plain" style={toneStyle("pink")}>
                      {ruleWord(violation.rule)}
                    </span>
                    <span className="faint small mono">{violation.rule}</span>
                  </p>
                  <p>
                    <RichText text={violationSentence(violation)} />
                  </p>
                  {violation.dataset === undefined ? undefined : (
                    <p className="faint small">
                      Dataset <span className="mono">{violation.dataset}</span>
                    </p>
                  )}
                </div>
                {range === undefined || file === undefined ? (
                  <p className="faint small">
                    {file === undefined
                      ? "The contract file is not available here."
                      : "This rule has no line of its own in the contract file."}
                  </p>
                ) : (
                  <YamlExcerpt file={file} path={path} range={range} />
                )}
              </div>
            );
          })}
        </div>
      )}
      {checked ? (
        <p className="faint small">
          Checked: {context?.datasetsChecked.map((dataset) => dataset).join(", ")}, as the files were when
          dag-healer looked at them.
        </p>
      ) : undefined}
    </section>
  );
};

const cellValue = (row: MaskedSample["rows"][number], column: string, index: number): unknown =>
  Array.isArray(row) ? row[index] : row[column];

const MASK = /^h:[0-9a-f]{6,}$|^\*{3}$/u;

/** The masked sample, with the columns of a failed check marked. */
export const SampleCard = ({ incident }: { readonly incident: IncidentDetail }) => {
  const id = useId();
  const sample = incident.sample;

  if (sample === null) {
    return null;
  }
  const first = sample.rows[0];
  const columns =
    sample.columns.length > 0
      ? sample.columns
      : first !== undefined && !Array.isArray(first)
        ? Object.keys(first)
        : [];
  const failed = new Set(incident.violations.map((violation) => violation.column).filter(Boolean));
  const marked = columns.filter((column) => failed.has(column));

  return (
    <section aria-labelledby={id} className="card" data-testid="sample">
      <div className="card-h">
        <h2 id={id}>Masked sample{sample.dataset ? ` of ${sample.dataset}` : ""}</h2>
        <span className="faint small">Personal values are masked before they are stored or shown.</span>
      </div>
      {marked.length > 0 ? (
        <p className="faint small">
          Highlighted: <RichText text={marked.map((column) => `\`${column}\``).join(", ")} />, named by a
          failed check.
        </p>
      ) : undefined}
      {sample.rows.length === 0 ? (
        <p className="muted">The sample has no rows.</p>
      ) : (
        <div className="card flush">
          <div className="scroll">
            <table className="tbl compact stackable data">
              <thead>
                <tr>
                  {columns.map((column) => (
                    <th className={failed.has(column) ? "hl mono" : "mono"} key={column} scope="col">
                      {column}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sample.rows.map((row, rowIndex) => (
                  // Sample rows are positional and never reordered.
                  <tr key={rowIndex}>
                    {columns.map((column, columnIndex) => {
                      const value = cellValue(row, column, columnIndex);
                      const empty = value === null || value === undefined || value === "";
                      const classes = [
                        "mono",
                        failed.has(column) ? "hl" : "",
                        !empty && MASK.test(String(value)) ? "masked" : "",
                      ];

                      return (
                        <td className={classes.filter(Boolean).join(" ")} data-label={column} key={column}>
                          {empty ? <span className="blank">empty</span> : formatValue(value)}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </section>
  );
};

/** What happened: the facts first, then dag-healer's reading of them. */
export const WhatHappened = ({ incident }: { readonly incident: IncidentDetail }) => {
  const id = useId();
  const { words } = useUiText();
  const context = contextOf(incident.events);
  const frame = context?.repoFrames.at(-1);
  const error =
    context?.exceptionType === undefined
      ? (incident.error_summary.split("\n")[0] ?? "").trim()
      : `${context.exceptionType}${context.exceptionMessage === undefined ? "" : `: ${context.exceptionMessage}`}`;

  return (
    <section aria-labelledby={id} className="card" data-testid="what-happened">
      <h2 className="lbl" id={id}>
        What happened
      </h2>
      <p className="lead">
        <RichText
          text={
            `\`${stepLabel(incident)}\` failed` +
            (error.length > 0 ? ` with \`${error}\`` : "") +
            (frame === undefined
              ? "."
              : ` in \`${frame.file}\`${frame.function === undefined ? "" : `, function \`${frame.function}\``}${
                  frame.line === undefined ? "" : `, line ${frame.line}`
                }.`)
          }
        />
      </p>
      <p className="muted">
        <RichText
          text={
            `${words.run.charAt(0).toUpperCase()}${words.run.slice(1)} \`${incident.run_id}\`, ${words.attempt} ` +
            `${incident.try_number}.` +
            (incident.occurrences > 1 ? ` The same failure was seen ${incident.occurrences} times.` : "")
          }
        />
      </p>
      {incident.diagnosis === null || incident.diagnosis.length === 0 ? undefined : (
        <div className="stack" style={{ gap: 4 }}>
          <h3 className="lbl">dag-healer's read</h3>
          <p>{incident.diagnosis}</p>
        </div>
      )}
    </section>
  );
};

/** The error as the orchestrator reported it (masked), folded away. */
export const ErrorDetails = ({ incident }: { readonly incident: IncidentDetail }) =>
  incident.error_summary.length === 0 ? null : (
    <details className="err" data-testid="error-details">
      <summary>Error and traceback</summary>
      <div className="err-body">
        <pre className="code wrap" tabIndex={0}>
          {incident.error_summary}
        </pre>
      </div>
    </details>
  );

/** The reasons behind the classification: the rule, its reason and the evidence. */
export const HowDecided = ({ incident }: { readonly incident: IncidentDetail }) => {
  const found = classificationOf(incident.events);
  const notes = contextOf(incident.events)?.notes ?? [];

  if (found === undefined && incident.evidence.length === 0) {
    return null;
  }

  return (
    <details className="more card" data-testid="how-decided">
      <summary>How dag-healer decided</summary>
      <div className="stack">
        {found === undefined ? undefined : (
          <p className="muted">
            Rule <span className="mono">{found.rule}</span>
            {found.reason === undefined ? "" : `: ${found.reason}`}
          </p>
        )}
        {incident.evidence.length === 0 ? undefined : (
          <ul className="plain-list">
            {incident.evidence.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        )}
        {notes.length === 0 ? undefined : (
          <ul className="plain-list">
            {notes.map((note) => (
              <li key={note}>{note}</li>
            ))}
          </ul>
        )}
      </div>
    </details>
  );
};
