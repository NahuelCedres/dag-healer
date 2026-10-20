import { Alert, Box, Card, Code, Heading, HStack, Spinner, Stack, Text } from "@chakra-ui/react";
import { useMemo } from "react";

import { type ContractView, type DagIncidentsResponse, getDagIncidents, hasSessionExpired } from "src/api";
import { ApiErrorNotice, SessionExpiredBanner } from "src/components/ApiErrorNotice";
import { ApproveContractButton, ContractStatusLine, DatasetRules } from "src/components/Contracts";
import { EmptyNotice } from "src/components/EmptyNotice";
import { IncidentTimeline } from "src/components/IncidentTimeline";
import { Section } from "src/components/Section";
import type { HealerContext } from "src/context/pluginContext";
import { usePolledResource, usePollingIntervals } from "src/hooks/usePolledResource";
import { NOT_MONITORED_MESSAGE } from "src/messages";
import { sortNewestFirst } from "src/model";

const ContractCard = ({
  contract,
  onQueued,
}: {
  readonly contract: ContractView | null;
  readonly onQueued: () => void;
}) => (
  <Card.Root data-testid="contract-card" size="sm" variant="outline">
    <Card.Header>
      <HStack gap={3} justifyContent="space-between" wrap="wrap">
        <Stack gap={1}>
          <Heading as="h3" size="md">
            Data contract
          </Heading>
          {contract === null ? undefined : <ContractStatusLine contract={contract} />}
        </Stack>
        {contract === null ? undefined : <ApproveContractButton contract={contract} onQueued={onQueued} />}
      </HStack>
    </Card.Header>
    <Card.Body>
      {contract === null ? (
        <Text color="fg.muted" fontSize="sm">
          This DAG has no data contract. Run <Code size="sm">dag-healer init</Code> to infer a draft from its
          datasets, then review and approve it.
        </Text>
      ) : (
        <Stack gap={3}>
          {contract.status === "approved" && !contract.stale ? undefined : (
            <Text color="fg.muted" fontSize="sm">
              Only approved contracts are enforced. Until this one is approved, data-shaped failures of this
              DAG wait for a human decision.
            </Text>
          )}
          {contract.path.length > 0 ? (
            <Text color="fg.muted" fontSize="sm">
              File <Code size="sm">{contract.path}</Code>
            </Text>
          ) : undefined}
          {contract.datasets.length === 0 ? (
            <Text color="fg.muted" fontSize="sm">
              The contract declares no datasets.
            </Text>
          ) : (
            contract.datasets.map((dataset) => <DatasetRules dataset={dataset} key={dataset.name} />)
          )}
        </Stack>
      )}
    </Card.Body>
  </Card.Root>
);

/** "Healer incidents" tab of a DAG: its data contract and its incidents. */
export const DagPanel = ({ context }: { readonly context: HealerContext }) => {
  const { dagId } = context;
  const { commandMs, refreshMs } = usePollingIntervals();
  const resource = usePolledResource<DagIncidentsResponse>(
    dagId === undefined ? null : `dag:${dagId}`,
    (signal) => getDagIncidents(dagId ?? "", signal),
    (data) => (data?.contract?.pending_approval ? commandMs : refreshMs),
  );
  const incidents = useMemo(() => sortNewestFirst(resource.data?.incidents ?? []), [resource.data]);

  if (dagId === undefined) {
    return <EmptyNotice title="Open a DAG to see its dag-healer incidents." />;
  }
  const { data, error } = resource;

  return (
    <Stack data-testid="dag-panel" gap={6}>
      {hasSessionExpired(error) ? <SessionExpiredBanner /> : undefined}
      <Box>
        <Heading as="h2" size="lg">
          Healer incidents
        </Heading>
        <Text color="fg.muted" fontFamily="mono" fontSize="sm">
          {dagId}
        </Text>
      </Box>

      {resource.loading ? (
        <HStack color="fg.muted" gap={2}>
          <Spinner size="sm" />
          <Text fontSize="sm">Loading…</Text>
        </HStack>
      ) : undefined}

      {error === undefined || error.status === 401 ? undefined : error.status === 404 ? (
        data === undefined ? (
          <EmptyNotice title="No dag-healer data for this DAG yet.">
            The dag-healer API returned 404 for this request.
          </EmptyNotice>
        ) : undefined
      ) : (
        <ApiErrorNotice error={error} />
      )}

      {data === undefined ? undefined : (
        <>
          {data.monitored ? undefined : (
            <Alert.Root data-testid="not-monitored" status="info">
              <Alert.Indicator />
              <Alert.Title>{NOT_MONITORED_MESSAGE}</Alert.Title>
            </Alert.Root>
          )}
          {data.monitored || data.contract !== null ? (
            <ContractCard contract={data.contract} onQueued={resource.refresh} />
          ) : undefined}
          {data.monitored || incidents.length > 0 ? (
            <Section title="Incidents">
              {incidents.length === 0 ? (
                <EmptyNotice title="No incidents recorded for this DAG." />
              ) : (
                <IncidentTimeline incidents={incidents} showDag={false} />
              )}
            </Section>
          ) : undefined}
        </>
      )}
    </Stack>
  );
};
