import { Badge, Box, Button, Code, HStack, Link, Stack, Table, Text } from "@chakra-ui/react";
import { useState } from "react";

import { approveContract, type ContractDatasetView, type ContractView, toApiError } from "src/api";
import { formatDateTime } from "src/format";
import { dagHref } from "src/links";
import { contractNeedsApproval } from "src/model";

import { ContractStatusBadge } from "./Badges";
import { ConfirmDialog } from "./ConfirmDialog";

const approvalError = (error: unknown): string => {
  const apiError = toApiError(error);

  if (apiError.status === 401) {
    return "Your Airflow session expired — reload the page and try again.";
  }
  if (apiError.status === 403) {
    return "You do not have permission to approve this contract.";
  }

  return apiError.detail;
};

/**
 * "Approve" button with its confirmation dialog, or the "Approval pending" badge while the worker has
 * not applied the queued command yet. Renders nothing when the user cannot approve.
 */
export const ApproveContractButton = ({
  contract,
  onQueued,
}: {
  readonly contract: ContractView;
  readonly onQueued?: () => void;
}) => {
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  // The contract object current when the POST succeeded: until a fresh one arrives from the API,
  // the approval is shown as pending even if the server has not reported it yet.
  const [queuedFor, setQueuedFor] = useState<ContractView>();

  if (contract.pending_approval || queuedFor === contract) {
    return (
      <Badge colorPalette="yellow" data-testid="approval-pending" variant="subtle">
        Approval pending
      </Badge>
    );
  }
  if (!contract.can_approve || !contractNeedsApproval(contract)) {
    return null;
  }

  const confirm = (note: string) => {
    setBusy(true);
    setError(undefined);
    approveContract(contract.dag_id, note)
      .then(() => {
        setQueuedFor(contract);
        setOpen(false);
        onQueued?.();
      })
      .catch((caught: unknown) => setError(approvalError(caught)))
      .finally(() => setBusy(false));
  };

  return (
    <>
      <Button onClick={() => setOpen(true)} size="xs" variant="outline">
        Approve
      </Button>
      {open ? (
        <ConfirmDialog
          busy={busy}
          confirmLabel="Approve contract"
          description={
            <>
              Approve the data contract of <Code size="sm">{contract.dag_id}</Code>? Once approved, dag-healer
              enforces its rules: failures caused by data that breaks them are classified as data incidents.
              The worker writes the approval to the contract YAML in the repository.
            </>
          }
          error={error}
          onCancel={() => {
            setOpen(false);
            setError(undefined);
          }}
          onConfirm={confirm}
          title="Approve contract?"
        />
      ) : undefined}
    </>
  );
};

const bound = (value: number | null): string => (value === null ? "" : String(value));

const rowBand = (dataset: ContractDatasetView): string => {
  const { max, min } = dataset.row_count;

  if (min === null && max === null) {
    return "any";
  }

  return `${min ?? 0} – ${max ?? "∞"}`;
};

/** One dataset of a contract: locator, row-count band and the column rules. */
export const DatasetRules = ({ dataset }: { readonly dataset: ContractDatasetView }) => (
  <Box borderColor="border" borderRadius="md" borderWidth="1px" data-testid="contract-dataset" p={3}>
    <HStack gap={3} mb={2} wrap="wrap">
      <Text fontWeight="semibold">{dataset.name}</Text>
      {dataset.task_id === null ? undefined : (
        <Text color="fg.muted" fontSize="sm">
          task <Code size="sm">{dataset.task_id}</Code>
        </Text>
      )}
      <Text color="fg.muted" fontSize="sm">
        {dataset.locator.type} <Code size="sm">{dataset.locator.path}</Code>
      </Text>
      <Text color="fg.muted" fontSize="sm">
        rows: {rowBand(dataset)}
      </Text>
    </HStack>
    {dataset.columns.length === 0 ? (
      <Text color="fg.muted" fontSize="sm">
        No column rules.
      </Text>
    ) : (
      <Table.ScrollArea maxWidth="100%">
        <Table.Root aria-label={`Column rules of ${dataset.name}`} size="sm" variant="line">
          <Table.Header>
            <Table.Row>
              <Table.ColumnHeader>Column</Table.ColumnHeader>
              <Table.ColumnHeader>Type</Table.ColumnHeader>
              <Table.ColumnHeader>Nullable</Table.ColumnHeader>
              <Table.ColumnHeader>Min</Table.ColumnHeader>
              <Table.ColumnHeader>Max</Table.ColumnHeader>
              <Table.ColumnHeader>PII</Table.ColumnHeader>
              <Table.ColumnHeader>Allowed values</Table.ColumnHeader>
              <Table.ColumnHeader>Note</Table.ColumnHeader>
            </Table.Row>
          </Table.Header>
          <Table.Body>
            {dataset.columns.map((column) => (
              <Table.Row key={column.name}>
                <Table.Cell fontFamily="mono">{column.name}</Table.Cell>
                <Table.Cell>{column.dtype}</Table.Cell>
                <Table.Cell>{column.nullable ? "yes" : "no"}</Table.Cell>
                <Table.Cell>{bound(column.min)}</Table.Cell>
                <Table.Cell>{bound(column.max)}</Table.Cell>
                <Table.Cell>{column.pii ? <Badge variant="surface">PII</Badge> : ""}</Table.Cell>
                <Table.Cell>{column.allowed_values?.join(", ") ?? ""}</Table.Cell>
                <Table.Cell color="fg.muted">{column.note ?? ""}</Table.Cell>
              </Table.Row>
            ))}
          </Table.Body>
        </Table.Root>
      </Table.ScrollArea>
    )}
  </Box>
);

/** Status line of a contract: badge, stale flag and approval. */
export const ContractStatusLine = ({ contract }: { readonly contract: ContractView }) => (
  <HStack gap={2} wrap="wrap">
    <ContractStatusBadge status={contract.status} />
    {contract.stale && contract.status !== "stale" ? <ContractStatusBadge status="stale" /> : undefined}
    {contract.approved_by === null ? undefined : (
      <Text color="fg.muted" fontSize="sm">
        approved by {contract.approved_by} on {formatDateTime(contract.approved_at)}
      </Text>
    )}
  </HStack>
);

/** The "Contracts" table of the Healer page. */
export const ContractsTable = ({
  contracts,
  onQueued,
}: {
  readonly contracts: Array<ContractView>;
  readonly onQueued?: () => void;
}) => (
  <Table.ScrollArea borderColor="border" borderRadius="md" borderWidth="1px" maxWidth="100%">
    <Table.Root aria-label="Contracts" size="sm" variant="line">
      <Table.Header>
        <Table.Row>
          <Table.ColumnHeader>DAG</Table.ColumnHeader>
          <Table.ColumnHeader>Status</Table.ColumnHeader>
          <Table.ColumnHeader>Datasets</Table.ColumnHeader>
          <Table.ColumnHeader>Approved by</Table.ColumnHeader>
          <Table.ColumnHeader>Approved at</Table.ColumnHeader>
          <Table.ColumnHeader>Stale</Table.ColumnHeader>
          <Table.ColumnHeader textAlign="end">Action</Table.ColumnHeader>
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {contracts.map((contract) => (
          <Table.Row data-dag-id={contract.dag_id} key={contract.dag_id}>
            <Table.Cell>
              <Link href={dagHref(contract.dag_id)}>{contract.dag_id}</Link>
            </Table.Cell>
            <Table.Cell>
              <ContractStatusBadge status={contract.status} />
            </Table.Cell>
            <Table.Cell>
              <Stack gap={0}>
                {contract.datasets.map((dataset) => (
                  <Text fontSize="sm" key={dataset.name}>
                    {dataset.name}{" "}
                    <Text as="span" color="fg.muted">
                      ({dataset.columns.length} columns)
                    </Text>
                  </Text>
                ))}
              </Stack>
            </Table.Cell>
            <Table.Cell>{contract.approved_by ?? ""}</Table.Cell>
            <Table.Cell>
              {contract.approved_at === null ? "" : formatDateTime(contract.approved_at)}
            </Table.Cell>
            <Table.Cell>{contract.stale ? <ContractStatusBadge status="stale" /> : "no"}</Table.Cell>
            <Table.Cell textAlign="end">
              <ApproveContractButton contract={contract} onQueued={onQueued} />
            </Table.Cell>
          </Table.Row>
        ))}
      </Table.Body>
    </Table.Root>
  </Table.ScrollArea>
);
