/** A pipeline's contract in plain words, next to its YAML file, with the approval when it is due. */
import { useId, useState } from "react";

import { approveContract, type ColumnRuleView, type ContractView } from "src/api";
import { capitalize, useUiText } from "src/vocabulary";

import { ConfirmPanel } from "./Confirm";
import { approvable } from "./PrimaryCard";
import { approvalConsequences } from "./consequences";
import { contractTone, contractWord, dtypeWord } from "./labels";
import { exactTime } from "./time";
import { Notice, RichText, toneStyle, useAnnounce } from "./ui";

const number = (value: number): string => value.toLocaleString("en-US");

/** What a column must hold, in plain words. */
export const columnSentence = (column: ColumnRuleView): string => {
  const parts = [dtypeWord(column.dtype), column.nullable ? "may be empty" : "never empty"];

  if (column.min !== null && column.max !== null) {
    parts.push(`between ${number(column.min)} and ${number(column.max)}`);
  } else if (column.min !== null) {
    parts.push(`at least ${number(column.min)}`);
  } else if (column.max !== null) {
    parts.push(`at most ${number(column.max)}`);
  }
  if (column.allowed_values !== null && column.allowed_values.length > 0) {
    parts.push(`one of ${column.allowed_values.map((value) => `\`${value}\``).join(", ")}`);
  }
  if (column.pii) {
    parts.push("personal data, masked in samples and alerts");
  }

  return parts.join(", ");
};

const statusSentence = (contract: ContractView): string => {
  switch (contract.status) {
    case "approved":
      return `Approved by ${contract.approved_by ?? "someone"}${
        contract.approved_at === null ? "" : ` on ${exactTime(contract.approved_at)}`
      }. A failed run whose input breaks it is DATA, without asking you.`;
    case "stale":
      return (
        "It changed after it was approved, so dag-healer treats it as unapproved: a data-shaped failure waits for a " +
        "person until someone approves it again."
      );
    default:
      return (
        "A draft, written by dag-healer init from a sample of the input and not reviewed yet: a data-shaped failure " +
        "waits for a person until someone approves it."
      );
  }
};

export const ContractCard = ({
  contract,
  file,
  onQueued,
  pipeline,
}: {
  /** null: the pipeline has no contract; undefined: not loaded. */
  readonly contract: ContractView | null | undefined;
  readonly file?: string;
  readonly onQueued: () => void;
  readonly pipeline: string;
}) => {
  const text = useUiText();
  const { words } = text;
  const id = useId();
  const announce = useAnnounce();
  const [tab, setTab] = useState<"file" | "plain">("plain");
  const [approving, setApproving] = useState(false);

  if (contract === undefined) {
    return null;
  }
  if (contract === null) {
    return (
      <section aria-labelledby={id} className="card" data-testid="contract">
        <h2 id={id}>Contract</h2>
        <p className="muted">
          <RichText
            text={
              `\`${pipeline}\` has no contract yet. \`dag-healer init\` drafts one from a sample of its input; until ` +
              "it is approved, a data-shaped failure waits for a person."
            }
          />
        </p>
      </section>
    );
  }

  return (
    <section aria-labelledby={id} className="card" data-testid="contract">
      <div className="card-h">
        <h2 className="mono" id={id}>
          {contract.path}
        </h2>
        <div className="row">
          <span className="pill plain" style={toneStyle(contractTone(contract.status))}>
            {contractWord(contract.status)}
          </span>
          <div aria-label="View" className="seg" role="group">
            <button aria-pressed={tab === "plain"} onClick={() => setTab("plain")} type="button">
              In plain words
            </button>
            <button aria-pressed={tab === "file"} onClick={() => setTab("file")} type="button">
              YAML file
            </button>
          </div>
        </div>
      </div>
      <p className="muted">{statusSentence(contract)}</p>
      {tab === "file" ? (
        file === undefined ? (
          <p className="muted">The file cannot be shown here.</p>
        ) : (
          <pre aria-label={contract.path} className="code" data-testid="contract-yaml" tabIndex={0}>
            {file}
          </pre>
        )
      ) : (
        contract.datasets.map((dataset) => (
          <div className="contract-ds" data-dataset={dataset.name} key={dataset.name}>
            {contract.datasets.length > 1 ? <h3>{dataset.name}</h3> : undefined}
            <ul className="plain-list">
              <li>
                <RichText
                  text={
                    `Checks \`${dataset.locator.path}\`` +
                    (dataset.task_id === null
                      ? "."
                      : `, the input of the ${words.step} \`${dataset.task_id}\`.`)
                  }
                />
              </li>
              {dataset.row_count.min === null && dataset.row_count.max === null ? undefined : (
                <li>
                  Expects{" "}
                  {dataset.row_count.min !== null && dataset.row_count.max !== null
                    ? `between ${number(dataset.row_count.min)} and ${number(dataset.row_count.max)} rows.`
                    : dataset.row_count.min === null
                      ? `at most ${number(dataset.row_count.max ?? 0)} rows.`
                      : `at least ${number(dataset.row_count.min)} rows.`}
                </li>
              )}
              <li>
                {dataset.allow_extra_columns === false
                  ? "Only these columns are allowed: an extra column breaks the contract."
                  : "Other columns are allowed; a missing one breaks the contract."}
              </li>
            </ul>
            <div className="card flush">
              <div className="scroll">
                <table className="tbl compact stackable">
                  <thead>
                    <tr>
                      <th scope="col">Column</th>
                      <th scope="col">What it must hold</th>
                    </tr>
                  </thead>
                  <tbody>
                    {dataset.columns.map((column) => (
                      <tr key={column.name}>
                        <td className="mono" data-label="Column">
                          {column.name}
                        </td>
                        <td className="muted wide" data-label="What it must hold">
                          <RichText text={capitalize(columnSentence(column))} />
                          {column.note === null ? undefined : (
                            <div className="faint small">{column.note}</div>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        ))
      )}
      <p className="faint small">
        The YAML file in your repository is what dag-healer checks; this page only reads it.
      </p>
      {contract.pending_approval ? (
        <Notice testId="approval-queued">
          <p>The approval is queued; dag-healer's worker applies it within a few seconds.</p>
        </Notice>
      ) : approvable(contract) ? (
        approving ? (
          <ConfirmPanel
            consequences={approvalConsequences(contract, text)}
            heading="If you approve the contract"
            onCancel={() => setApproving(false)}
            onConfirm={async (note) => {
              await approveContract(contract.dag_id, note);
              setApproving(false);
              announce("Queued: approve the contract.");
              onQueued();
            }}
          />
        ) : (
          <div className="row">
            <button
              className="btn"
              data-action="approve_contract"
              onClick={() => setApproving(true)}
              type="button"
            >
              Approve the contract
            </button>
            <span className="faint small">Read it in plain words and in the file first.</span>
          </div>
        )
      ) : undefined}
    </section>
  );
};
