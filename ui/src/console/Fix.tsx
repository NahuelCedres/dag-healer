/**
 * The fix of a CODE incident: the branch, the test, the test results before and after the fix,
 * the diff, the pull request or PR.md, and the CI note.
 */
import { useEffect, useId, useState } from "react";

import {
  getPrMarkdown,
  hasAuthorizationProvider,
  type IncidentDetail,
  type SetupResponse,
  toApiError,
} from "src/api";
import { type DiffLine, parseDiff } from "src/diff";
import { prMarkdownHref } from "src/links";

import { Modal } from "./Modal";
import { repairOf } from "./events";
import { ciSection, markdownBlocks, type PytestRun, pytestResults } from "./texts";
import { CopyButton, Icon, Notice, RichText, toneStyle } from "./ui";

export interface FileDiff {
  readonly added: number;
  readonly isNew: boolean;
  readonly isTest: boolean;
  readonly lines: Array<DiffLine>;
  readonly path: string;
  readonly removed: number;
}

const TEST_PATH = /(^|\/)tests?\/|(^|\/)test_[^/]*\.py$|_test\.py$/u;

/** The files of a unified diff, each with its lines and counts. */
export const fileDiffs = (diff: string | null | undefined, testPath?: string | null): Array<FileDiff> => {
  const files: Array<{ isNew: boolean; lines: Array<DiffLine>; path: string }> = [];

  for (const line of parseDiff(diff)) {
    if (line.kind === "file") {
      const match = /^diff --git a\/(.+?) b\/(.+)$/u.exec(line.text);

      files.push({ isNew: false, lines: [], path: match?.[2] ?? line.text });
      continue;
    }
    let current = files.at(-1);

    if (current === undefined) {
      // A diff without a `diff --git` header: one file, named by its `+++` line.
      current = { isNew: false, lines: [], path: "" };
      files.push(current);
    }
    if (line.kind === "meta") {
      if (line.text.startsWith("new file mode")) {
        current.isNew = true;
      }
      if (current.path === "" && line.text.startsWith("+++ ")) {
        current.path = line.text.slice(4).replace(/^b\//u, "").trim();
      }
      continue;
    }
    current.lines.push(line);
  }

  return files.map((file) => ({
    ...file,
    added: file.lines.filter((line) => line.kind === "add").length,
    isTest: file.path === testPath || TEST_PATH.test(file.path),
    removed: file.lines.filter((line) => line.kind === "del").length,
  }));
};

const FileDiffView = ({ file }: { readonly file: FileDiff }) => (
  <div className="diff" data-testid="diff-file">
    <div className="diff-h">
      <span className="mono wrap-anywhere">{file.path}</span>
      <span className="faint mono">
        {file.isNew ? "new file · " : ""}+{file.added} −{file.removed}
      </span>
    </div>
    <div className="diff-b" tabIndex={0}>
      {file.lines.map((line, index) => (
        // Diff lines are positional and never reordered.
        <div className={`l ${line.kind === "context" ? "" : line.kind}`} key={index}>
          {line.text.length === 0 ? " " : line.text}
        </div>
      ))}
    </div>
  </div>
);

const RunPill = ({ label, run }: { readonly label: string; readonly run: PytestRun }) => (
  <span className="pill plain" style={toneStyle(run.passed ? "lime" : "pink")}>
    {label}: {run.summary}
  </span>
);

/** PR.md in a dialog, with a copy button and the path of the file on the worker. */
const PrMarkdownDialog = ({
  incident,
  onClose,
  path,
  text,
}: {
  readonly incident: IncidentDetail;
  readonly onClose: () => void;
  readonly path?: string;
  readonly text: string;
}) => (
  <Modal
    actions={<CopyButton label="PR.md" text={text} />}
    onClose={onClose}
    subtitle={path === undefined ? undefined : `On the worker: ${path}`}
    title={`PR.md · ${incident.dag_id} / ${incident.task_id}`}
  >
    <pre className="code wrap" tabIndex={0}>
      {text}
    </pre>
  </Modal>
);

export const FixCard = ({
  incident,
  setup,
}: {
  readonly incident: IncidentDetail;
  readonly setup?: SetupResponse;
}) => {
  const id = useId();
  const repair = repairOf(incident.events);
  const files = fileDiffs(incident.diff, incident.test_path);
  const code = files.filter((file) => !file.isTest);
  const tests = files.filter((file) => file.isTest);
  const results = pytestResults(incident.pytest_output);
  const hasPrMd = incident.pr_md_url !== null || incident.has_pr_md;
  const [prMd, setPrMd] = useState<{ error?: string; text?: string }>({});
  const [showPrMd, setShowPrMd] = useState(false);
  const failed = incident.status === "repair_failed";

  useEffect(() => {
    if (!hasPrMd) {
      return undefined;
    }
    const controller = new AbortController();

    getPrMarkdown(incident.dag_id, incident.id, controller.signal)
      .then((body) => setPrMd({ text: typeof body === "string" ? body : JSON.stringify(body, undefined, 2) }))
      .catch((caught: unknown) => {
        if (!controller.signal.aborted) {
          setPrMd({ error: toApiError(caught).detail });
        }
      });

    return () => controller.abort();
  }, [hasPrMd, incident.dag_id, incident.id]);

  if (incident.diff === null && incident.test_code === null && incident.pr_url === null && !hasPrMd) {
    return null;
  }
  const ci = ciSection(prMd.text);
  const total = files.reduce(
    (sum, file) => ({ added: sum.added + file.added, removed: sum.removed + file.removed }),
    {
      added: 0,
      removed: 0,
    },
  );

  return (
    <section aria-labelledby={id} className="card" data-testid="fix">
      <div className="card-h">
        <h2 id={id}>{failed ? "What the repair tried" : "The fix"}</h2>
        {files.length === 0 ? undefined : (
          <span className="faint small mono">
            {files.length} {files.length === 1 ? "file" : "files"} changed, +{total.added} −{total.removed}
          </span>
        )}
      </div>
      {failed && repair?.error !== undefined ? (
        <Notice tone="error">
          <p>
            <RichText text={repair.error} />
          </p>
        </Notice>
      ) : undefined}
      <dl className="kv">
        {repair?.branch === undefined ? undefined : (
          <>
            <dt>Branch</dt>
            <dd className="row">
              <span className="mono wrap-anywhere">{repair.branch}</span>
              <CopyButton label="Branch" text={repair.branch} />
            </dd>
          </>
        )}
        {incident.test_path === null ? undefined : (
          <>
            <dt>Test</dt>
            <dd className="mono">{incident.test_path}</dd>
          </>
        )}
        {code.length === 0 ? undefined : (
          <>
            <dt>Changes</dt>
            <dd className="mono">{code.map((file) => file.path).join(", ")}</dd>
          </>
        )}
      </dl>
      {results.before === undefined && results.after === undefined ? undefined : (
        <div className="stack" style={{ gap: 6 }}>
          <div className="tests" data-testid="test-results">
            <span className="lbl">Tests</span>
            {results.before === undefined ? undefined : (
              <RunPill label="before the fix" run={results.before} />
            )}
            {results.after === undefined ? undefined : <RunPill label="after" run={results.after} />}
          </div>
          <p className="faint small">
            {results.before !== undefined && !results.before.passed
              ? "The new test failed before the fix, as it should: it reproduces the failure. "
              : ""}
            {results.after?.passed === true ? "With the fix, the tests pass. " : ""}
            They ran in the repair sandbox, not in your CI.
          </p>
        </div>
      )}
      {code.map((file) => (
        <FileDiffView file={file} key={file.path} />
      ))}
      {tests.length > 0 ? (
        <details className="more">
          <summary>
            Show the test{tests.length > 1 ? "s" : ""}: {tests.map((file) => file.path).join(", ")}
          </summary>
          <div className="stack">
            {tests.map((file) => (
              <FileDiffView file={file} key={file.path} />
            ))}
          </div>
        </details>
      ) : incident.test_code === null ? undefined : (
        <details className="more">
          <summary>Show the test{incident.test_path === null ? "" : `: ${incident.test_path}`}</summary>
          <pre className="code" tabIndex={0}>
            {incident.test_code}
          </pre>
        </details>
      )}
      <div className="stack" style={{ gap: 8 }}>
        <h3 className="lbl">Pull request</h3>
        {incident.pr_url === null ? undefined : (
          <p>
            <a href={incident.pr_url} rel="noopener noreferrer" target="_blank">
              <Icon name="out" size={14} /> Open the pull request
            </a>
          </p>
        )}
        {hasPrMd ? (
          <>
            <p className="muted">
              {incident.pr_url === null
                ? "No pull request was opened: dag-healer committed the fix on its branch and described it in PR.md, " +
                  "with fix.patch next to it."
                : "The pull request's description is also kept as PR.md."}
              {repair?.prMdPath === undefined ? "" : " On the worker: "}
              {repair?.prMdPath === undefined ? undefined : (
                <span className="mono wrap-anywhere">{repair.prMdPath}</span>
              )}
            </p>
            <div className="row">
              <button
                className="btn sm"
                disabled={prMd.text === undefined}
                onClick={() => setShowPrMd(true)}
                type="button"
              >
                <Icon name="doc" size={14} />
                View PR.md
              </button>
              {/* A new tab cannot send the standalone host's API token: then only the dialog reads PR.md. */}
              {hasAuthorizationProvider() ? undefined : (
                <a
                  className="btn sm ghost"
                  href={prMarkdownHref(incident.dag_id, incident.id)}
                  rel="noopener"
                  target="_blank"
                >
                  Open PR.md in a new tab
                </a>
              )}
            </div>
            {prMd.error === undefined ? undefined : (
              <p className="faint small">PR.md cannot be read: {prMd.error}</p>
            )}
          </>
        ) : incident.pr_url === null ? (
          <p className="muted">
            No pull request or PR.md was written{failed ? ": the repair did not finish" : ""}.
          </p>
        ) : undefined}
      </div>
      {ci !== undefined ? (
        <div className="stack" data-testid="ci-note" style={{ gap: 6 }}>
          <h3 className="lbl">CI</h3>
          {markdownBlocks(ci).map((block, index) =>
            "code" in block ? (
              // Blocks are positional and never reordered.
              <pre className="code wrap" key={index} tabIndex={0}>
                {block.code}
              </pre>
            ) : (
              <p className="muted" key={index}>
                <RichText text={block.text} />
              </p>
            ),
          )}
        </div>
      ) : setup !== undefined && !failed ? (
        <div className="stack" data-testid="ci-note" style={{ gap: 6 }}>
          <h3 className="lbl">CI</h3>
          <p className="muted">
            {setup.repairs.skip_ci
              ? "The fix's commit carries [skip ci]: your CI does not run the model-written test and fix before a person " +
                "has reviewed them."
              : "Your CI runs on this branch as soon as it is pushed (github.skip_ci is off)."}
          </p>
        </div>
      ) : undefined}
      {incident.pytest_output === null || incident.pytest_output.length === 0 ? undefined : (
        <details className="more">
          <summary>Show the pytest output</summary>
          <pre className="code" tabIndex={0}>
            {incident.pytest_output}
          </pre>
        </details>
      )}
      {repair !== undefined && repair.notes.length > 0 ? (
        <ul className="plain-list">
          {repair.notes.map((note) => (
            <li key={note}>
              <RichText text={note} />
            </li>
          ))}
        </ul>
      ) : undefined}
      {showPrMd && prMd.text !== undefined ? (
        <PrMarkdownDialog
          incident={incident}
          onClose={() => setShowPrMd(false)}
          path={repair?.prMdPath}
          text={prMd.text}
        />
      ) : undefined}
    </section>
  );
};
