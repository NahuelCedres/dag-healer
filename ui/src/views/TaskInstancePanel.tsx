import {
  Alert,
  Box,
  Button,
  Code,
  Collapsible,
  DataList,
  Heading,
  HStack,
  Link,
  List,
  Spinner,
  Stack,
  Table,
  Text,
} from "@chakra-ui/react";
import { type ReactNode, useState } from "react";

import {
  type AlertRecord,
  getPrMarkdown,
  getTaskInstanceIncident,
  hasAuthorizationProvider,
  hasSessionExpired,
  type IncidentAction,
  type IncidentDetail,
  runIncidentAction,
  type TaskInstanceIncidentResponse,
  toApiError,
} from "src/api";
import { ApiErrorNotice, SessionExpiredBanner } from "src/components/ApiErrorNotice";
import { CategoryBadge, StatusBadge } from "src/components/Badges";
import { CodeBlock } from "src/components/CodeBlock";
import { ConfirmDialog } from "src/components/ConfirmDialog";
import { DiffView } from "src/components/DiffView";
import { EmptyNotice } from "src/components/EmptyNotice";
import { EventTimeline } from "src/components/EventTimeline";
import { MaskedSampleTable } from "src/components/MaskedSampleTable";
import { Section } from "src/components/Section";
import { ViolatedRuleCard, ViolationsTable } from "src/components/ViolationsTable";
import type { HealerContext } from "src/context/pluginContext";
import { actionLabel, formatConfidence, formatDateTime, ruleLabel, taskLabel } from "src/format";
import { usePolledResource, usePollingIntervals } from "src/hooks/usePolledResource";
import { prMarkdownHref } from "src/links";
import { noStepInstanceIncidentMessage, QUEUED_MESSAGE, sessionExpiredRetryMessage } from "src/messages";
import { actionWarnings, decisionFromEvents, nativeLinks } from "src/model";
import { DEFAULT_VOCABULARY, type UiText, useUiText, type Vocabulary } from "src/vocabulary";

/** Order of the buttons in the action bar. */
const ACTION_ORDER: ReadonlyArray<IncidentAction> = [
  "resolve_data",
  "resolve_code",
  "retry_repair",
  "retry_task",
  "ignore",
];

const ACTION_DESCRIPTIONS: Record<IncidentAction, string> = {
  ignore: "Close the incident without further action. It stays in the history as ignored.",
  resolve_code:
    "Treat this failure as a code bug: dag-healer writes a failing test, attempts a minimal fix and " +
    "opens a pull request (or writes PR.md when no GitHub token is configured).",
  resolve_data:
    "Treat this failure as a data problem: dag-healer reports the violated rule with a masked sample " +
    "and alerts the data team. No code is changed.",
  retry_repair: "Run the repair agent again on a fresh clone of the repository.",
  retry_task:
    "Clear the failed task instance in Airflow so the scheduler runs it again, then the tasks after it " +
    "that failed or were blocked by it. A code fix only takes effect once its branch is merged.",
};

/** What an action does; outside Airflow, `retry_task` is the orchestrator's own remediation. */
const actionDescription = (action: IncidentAction, words: Vocabulary): string =>
  action === "retry_task" && words.product !== DEFAULT_VOCABULARY.product
    ? `${words.retry_action}: ${words.product} runs the failed ${words.step} again. A code fix only ` +
      "takes effect once its branch is merged."
    : ACTION_DESCRIPTIONS[action];

const actionError = (error: unknown, text: UiText): string => {
  const apiError = toApiError(error);

  switch (apiError.status) {
    case 401:
      return sessionExpiredRetryMessage(text);
    case 403:
      return "You do not have permission to change this incident.";
    case 409:
      return `This action is no longer allowed for the current status: ${apiError.detail}`;
    default:
      return apiError.detail;
  }
};

const Field = ({ children, label }: { readonly children: ReactNode; readonly label: string }) => (
  <DataList.Item>
    <DataList.ItemLabel>{label}</DataList.ItemLabel>
    <DataList.ItemValue>{children}</DataList.ItemValue>
  </DataList.Item>
);

const ClassificationSection = ({ incident }: { readonly incident: IncidentDetail }) => {
  const decision = decisionFromEvents(incident.events);

  return (
    <Section title="Classification">
      <DataList.Root data-testid="classification" orientation="horizontal" size="sm">
        <Field label="Category">
          <CategoryBadge category={incident.category} />
        </Field>
        <Field label="Confidence">{formatConfidence(incident.confidence)}</Field>
        <Field label="Status">
          <StatusBadge status={incident.status} />
        </Field>
        <Field label="Source">{incident.source}</Field>
        {decision === undefined ? undefined : (
          <>
            <Field label="Decision rule">
              <Stack gap={0}>
                <Text>{ruleLabel(decision.rule ?? "")}</Text>
                <Code size="sm" width="fit-content">
                  {decision.rule}
                </Code>
              </Stack>
            </Field>
            {decision.reason === undefined ? undefined : <Field label="Reason">{decision.reason}</Field>}
          </>
        )}
        {incident.resolution === null ? undefined : (
          <Field label="Resolution">
            {incident.resolution}
            {incident.resolved_by === null ? "" : ` by ${incident.resolved_by}`}
          </Field>
        )}
        <Field label="Repair attempts">{incident.attempts}</Field>
        <Field label="Tokens used">{incident.tokens_used.toLocaleString("en-US")}</Field>
        <Field label="Detected">{formatDateTime(incident.created_at)}</Field>
        <Field label="Updated">{formatDateTime(incident.updated_at)}</Field>
      </DataList.Root>
    </Section>
  );
};

const PytestOutput = ({ output }: { readonly output: string }) => {
  const [open, setOpen] = useState(false);

  return (
    <Collapsible.Root lazyMount onOpenChange={(details) => setOpen(details.open)} open={open}>
      <Collapsible.Trigger asChild>
        <Button size="xs" variant="outline">
          {open ? "Hide pytest output" : "Show pytest output"}
        </Button>
      </Collapsible.Trigger>
      <Collapsible.Content>
        <CodeBlock label="pytest output" mt={2}>
          {output}
        </CodeBlock>
      </Collapsible.Content>
    </Collapsible.Root>
  );
};

const PrMarkdownPreview = ({ incident }: { readonly incident: IncidentDetail }) => {
  const [text, setText] = useState<string>();
  const [error, setError] = useState<string>();
  const [open, setOpen] = useState(false);

  const toggle = () => {
    const next = !open;

    setOpen(next);
    if (next && text === undefined) {
      getPrMarkdown(incident.dag_id, incident.id)
        .then((body) => setText(typeof body === "string" ? body : JSON.stringify(body, undefined, 2)))
        .catch((caught: unknown) => setError(toApiError(caught).detail));
    }
  };

  return (
    <Stack gap={2}>
      <Button onClick={toggle} size="xs" variant="ghost" width="fit-content">
        {open ? "Hide PR.md" : "Preview PR.md here"}
      </Button>
      {open && error !== undefined ? (
        <Text color="fg.error" fontSize="sm">
          {error}
        </Text>
      ) : undefined}
      {open && text !== undefined ? <CodeBlock label="PR.md">{text}</CodeBlock> : undefined}
    </Stack>
  );
};

const PullRequestSection = ({ incident }: { readonly incident: IncidentDetail }) => {
  const hasPrMd = incident.pr_md_url !== null || incident.has_pr_md;

  if (incident.pr_url === null && !hasPrMd) {
    return null;
  }

  return (
    <Section title="Pull request">
      <Stack gap={2}>
        {incident.pr_url === null ? undefined : (
          <Link href={incident.pr_url} rel="noopener noreferrer" target="_blank">
            Open pull request
          </Link>
        )}
        {hasPrMd ? (
          <>
            <Text color="fg.muted" fontSize="sm">
              {incident.pr_url === null
                ? "No GitHub token was configured, so the fix was pushed to a local branch and described in PR.md."
                : "The pull request description is also available as PR.md."}
            </Text>
            {/* A new tab cannot send the standalone host's API token: then only the preview reads PR.md. */}
            {hasAuthorizationProvider() ? undefined : (
              <Link href={prMarkdownHref(incident.dag_id, incident.id)} rel="noopener" target="_blank">
                View PR.md
              </Link>
            )}
            <PrMarkdownPreview incident={incident} />
          </>
        ) : undefined}
      </Stack>
    </Section>
  );
};

const AlertsTable = ({ alerts }: { readonly alerts: Array<AlertRecord> }) => (
  <Table.ScrollArea borderColor="border" borderRadius="md" borderWidth="1px" maxWidth="100%">
    <Table.Root aria-label="Alerts sent" size="sm" variant="line">
      <Table.Header>
        <Table.Row>
          <Table.ColumnHeader>Sent at</Table.ColumnHeader>
          <Table.ColumnHeader>Channel</Table.ColumnHeader>
          <Table.ColumnHeader>Recipients</Table.ColumnHeader>
          <Table.ColumnHeader>Subject</Table.ColumnHeader>
          <Table.ColumnHeader>Status</Table.ColumnHeader>
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {alerts.map((alert, index) => (
          <Table.Row key={`${alert.sent_at}-${alert.channel}-${index}`}>
            <Table.Cell whiteSpace="nowrap">{formatDateTime(alert.sent_at)}</Table.Cell>
            <Table.Cell>{alert.channel}</Table.Cell>
            <Table.Cell>{(alert.recipients ?? []).join(", ")}</Table.Cell>
            <Table.Cell>{alert.subject}</Table.Cell>
            <Table.Cell>
              <Text
                color={
                  alert.status === "failed" ? "fg.error" : alert.status === "sent" ? "fg.success" : "fg.muted"
                }
                fontSize="sm"
              >
                {alert.status}
              </Text>
              {alert.error === null ? undefined : (
                <Text color="fg.muted" fontSize="xs">
                  {alert.error}
                </Text>
              )}
            </Table.Cell>
          </Table.Row>
        ))}
      </Table.Body>
    </Table.Root>
  </Table.ScrollArea>
);

/** A queued command's kind as the view words it: `retry_task` is the orchestrator's retry action. */
const commandLabel = (kind: string, words: Vocabulary): string =>
  kind === "retry_task" ? words.retry_action.toLowerCase() : kind.replaceAll("_", " ");

const PendingNotice = ({ incident }: { readonly incident: IncidentDetail }) => {
  const { words } = useUiText();

  return (
    <Alert.Root data-testid="queued" status="info">
      <Spinner size="sm" />
      <Alert.Content>
        <Alert.Title>{QUEUED_MESSAGE}</Alert.Title>
        {incident.pending_commands.length > 0 ? (
          <Alert.Description>
            {incident.pending_commands
              .map((command) => `${commandLabel(command.kind, words)} by ${command.actor}`)
              .join("; ")}
          </Alert.Description>
        ) : undefined}
      </Alert.Content>
    </Alert.Root>
  );
};

const ActionBar = ({
  disabled,
  incident,
  onQueued,
}: {
  readonly disabled: boolean;
  readonly incident: IncidentDetail;
  readonly onQueued: () => void;
}) => {
  const [action, setAction] = useState<IncidentAction>();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const text = useUiText();
  const actions = ACTION_ORDER.filter((candidate) => incident.allowed_actions.includes(candidate));

  if (!incident.can_edit || actions.length === 0) {
    return null;
  }

  const close = () => {
    setAction(undefined);
    setError(undefined);
  };

  const confirm = (note: string) => {
    if (action === undefined) {
      return;
    }
    setBusy(true);
    setError(undefined);
    runIncidentAction(incident.dag_id, incident.id, action, note)
      .then(() => {
        setAction(undefined);
        onQueued();
      })
      .catch((caught: unknown) => setError(actionError(caught, text)))
      .finally(() => setBusy(false));
  };

  return (
    <Box
      as="nav"
      aria-label="Incident actions"
      bg="bg.subtle"
      borderColor="border"
      borderRadius="md"
      borderWidth="1px"
      data-testid="action-bar"
      p={3}
    >
      <HStack gap={2} wrap="wrap">
        <Text fontSize="sm" fontWeight="semibold" me={2}>
          Actions
        </Text>
        {actions.map((candidate) => (
          <Button
            disabled={disabled || busy}
            key={candidate}
            onClick={() => setAction(candidate)}
            size="sm"
            variant={candidate === "ignore" ? "ghost" : "outline"}
          >
            {actionLabel(candidate, text.words)}
          </Button>
        ))}
      </HStack>
      {action === undefined ? undefined : (
        <ConfirmDialog
          busy={busy}
          confirmLabel={actionLabel(action, text.words)}
          description={actionDescription(action, text.words)}
          error={error}
          onCancel={close}
          onConfirm={confirm}
          title={`${actionLabel(action, text.words)}?`}
          warnings={actionWarnings(incident, action)}
        />
      )}
    </Box>
  );
};

const IncidentView = ({
  incident,
  onQueued,
  queued,
}: {
  readonly incident: IncidentDetail;
  readonly onQueued: () => void;
  readonly queued: boolean;
}) => {
  const { words } = useUiText();
  const pending = queued || incident.pending_commands.length > 0;
  const native = nativeLinks(incident);

  return (
    <Stack data-incident-id={incident.id} gap={8}>
      <Stack gap={2}>
        <HStack gap={3} wrap="wrap">
          <Heading as="h2" size="lg">
            Healer incident
          </Heading>
          <CategoryBadge category={incident.category} size="md" />
          <StatusBadge size="md" status={incident.status} />
        </HStack>
        <Text color="fg.muted" fontSize="sm">
          {incident.dag_id} · {taskLabel(incident.task_id, incident.map_index)} · {words.attempt}{" "}
          {incident.try_number}
          {incident.occurrences > 1 ? ` · seen ${incident.occurrences} times` : ""} · run {incident.run_id}
        </Text>
        {native.length === 0 ? undefined : (
          <HStack data-testid="native-links" fontSize="sm" gap={4} wrap="wrap">
            {native.map((link) => (
              <Link href={link.url} key={link.label} rel="noopener noreferrer" target="_blank">
                {`Open in ${link.label}`}
              </Link>
            ))}
          </HStack>
        )}
      </Stack>

      {pending ? <PendingNotice incident={incident} /> : undefined}
      <ActionBar disabled={pending} incident={incident} onQueued={onQueued} />

      <ClassificationSection incident={incident} />

      <Section title="Diagnosis">
        <Stack gap={3}>
          <Text data-testid="diagnosis" whiteSpace="pre-wrap">
            {incident.diagnosis ?? "No diagnosis yet."}
          </Text>
          {incident.evidence.length > 0 ? (
            <Box>
              <Text color="fg.muted" fontSize="sm" mb={1}>
                Evidence
              </Text>
              <List.Root fontSize="sm" ps={5}>
                {incident.evidence.map((item) => (
                  <List.Item key={item}>{item}</List.Item>
                ))}
              </List.Root>
            </Box>
          ) : undefined}
          {incident.error_summary.length > 0 ? (
            <Box>
              <Text color="fg.muted" fontSize="sm" mb={1}>
                Error (masked)
              </Text>
              <CodeBlock label="Error summary" maxHeight="12rem" whiteSpace="pre-wrap">
                {incident.error_summary}
              </CodeBlock>
            </Box>
          ) : undefined}
        </Stack>
      </Section>

      {incident.violated_rule === null && incident.violations.length === 0 ? undefined : (
        <Section title="Violated rule">
          <Stack gap={3}>
            {incident.violated_rule === null ? undefined : <ViolatedRuleCard rule={incident.violated_rule} />}
            {incident.violations.length > 0 ? (
              <ViolationsTable violations={incident.violations} />
            ) : undefined}
          </Stack>
        </Section>
      )}

      {incident.sample === null ? undefined : (
        <Section title="Masked data sample">
          <MaskedSampleTable sample={incident.sample} />
        </Section>
      )}

      {incident.test_code === null ? undefined : (
        <Section title="Generated test">
          <Stack gap={2}>
            {incident.test_path === null ? undefined : (
              <Code data-testid="test-path" size="sm" width="fit-content">
                {incident.test_path}
              </Code>
            )}
            <CodeBlock label="Generated test">{incident.test_code}</CodeBlock>
          </Stack>
        </Section>
      )}

      {incident.diff === null || incident.diff.length === 0 ? undefined : (
        <Section title="Fix">
          <DiffView diff={incident.diff} />
        </Section>
      )}

      {incident.pytest_output === null || incident.pytest_output.length === 0 ? undefined : (
        <Section title="Pytest output">
          <PytestOutput output={incident.pytest_output} />
        </Section>
      )}

      <PullRequestSection incident={incident} />

      <Section title="Alerts sent">
        {incident.alerts.length === 0 ? (
          <Text color="fg.muted" fontSize="sm">
            No alerts sent.
          </Text>
        ) : (
          <AlertsTable alerts={incident.alerts} />
        )}
      </Section>

      <Section title="Timeline">
        {incident.events.length === 0 ? (
          <Text color="fg.muted" fontSize="sm">
            No events recorded.
          </Text>
        ) : (
          <EventTimeline events={incident.events} />
        )}
      </Section>
    </Stack>
  );
};

/** "Healer incident" tab of a task instance: classification, evidence, fix and human actions. */
export const TaskInstancePanel = ({ context }: { readonly context: HealerContext }) => {
  const { dagId, mapIndex, runId, taskId } = context;
  const { words } = useUiText();
  const { commandMs, refreshMs } = usePollingIntervals();
  const ready = dagId !== undefined && runId !== undefined && taskId !== undefined;
  const key = ready ? JSON.stringify(["ti", dagId, runId, taskId, mapIndex ?? "-1"]) : null;
  const resource = usePolledResource<TaskInstanceIncidentResponse>(
    key,
    (signal) =>
      getTaskInstanceIncident(
        dagId ?? "",
        { mapIndex: mapIndex ?? -1, runId: runId ?? "", taskId: taskId ?? "" },
        signal,
      ),
    (data) => ((data?.incident?.pending_commands.length ?? 0) > 0 ? commandMs : refreshMs),
  );
  // The incident snapshot current when a command was queued: until a fresh one arrives, the command
  // is shown as queued even if the server has not listed it in pending_commands yet.
  const [queuedFor, setQueuedFor] = useState<IncidentDetail>();

  if (!ready) {
    return <EmptyNotice title={`Open a ${words.step} instance to see its dag-healer incident.`} />;
  }
  const { data, error } = resource;
  const incident = data?.incident ?? null;

  return (
    <Stack data-testid="task-instance-panel" gap={6}>
      {hasSessionExpired(error) ? <SessionExpiredBanner /> : undefined}
      {resource.loading ? (
        <HStack color="fg.muted" gap={2}>
          <Spinner size="sm" />
          <Text fontSize="sm">Loading…</Text>
        </HStack>
      ) : undefined}
      {error === undefined || error.status === 401 || error.status === 404 ? undefined : (
        <ApiErrorNotice error={error} />
      )}
      {incident === null ? (
        resource.loading || (error !== undefined && error.status !== 404) ? undefined : (
          <EmptyNotice title={noStepInstanceIncidentMessage(words)}>
            dag-healer records an incident when this {words.step} instance fails.
          </EmptyNotice>
        )
      ) : (
        <IncidentView
          incident={incident}
          onQueued={() => {
            setQueuedFor(incident);
            resource.refresh();
          }}
          queued={queuedFor === incident}
        />
      )}
    </Stack>
  );
};
