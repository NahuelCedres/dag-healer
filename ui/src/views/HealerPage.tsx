import {
  Badge,
  Box,
  Heading,
  HStack,
  Link,
  NativeSelect,
  SimpleGrid,
  Spinner,
  Stack,
  Stat,
  Table,
  Text,
} from "@chakra-ui/react";
import { useMemo, useState } from "react";

import {
  type ApiError,
  CATEGORIES,
  type ContractListResponse,
  type DagListResponse,
  type DagSummary,
  hasSessionExpired,
  type IncidentListResponse,
  listContracts,
  listDags,
  listIncidents,
} from "src/api";
import { ApiErrorNotice, SessionExpiredBanner } from "src/components/ApiErrorNotice";
import { CategoryBadge, ContractStatusBadge, StatusBadge } from "src/components/Badges";
import { ContractsTable } from "src/components/Contracts";
import { EmptyNotice } from "src/components/EmptyNotice";
import { IncidentTimeline } from "src/components/IncidentTimeline";
import { Section } from "src/components/Section";
import { categoryLabel, formatDateTime, statusLabel } from "src/format";
import { usePolledResource, usePollingIntervals } from "src/hooks/usePolledResource";
import { dagHref, taskInstanceHref } from "src/links";
import { countIncidents, orderStatuses, sortNewestFirst } from "src/model";
import { categoryPalette } from "src/palettes";

/** Incidents fetched for the page; counts and filters are computed client-side from them. */
export const INCIDENT_PAGE_LIMIT = 500;

const ALL = "";
const UNCLASSIFIED = "none";

const STAT_TILES: ReadonlyArray<{ label: string; statuses: ReadonlyArray<string> }> = [
  { label: "Waiting for a human", statuses: ["waiting_human"] },
  { label: "Repaired", statuses: ["repaired"] },
  { label: "Reported", statuses: ["reported"] },
  { label: "Repair failed", statuses: ["repair_failed", "error"] },
];

const NotFoundNotice = ({ title }: { readonly title: string }) => (
  <EmptyNotice title={title}>The dag-healer API returned 404 for this request.</EmptyNotice>
);

/** Error of a section: 401 is shown once at the top of the page, 404 as an empty state. */
const SectionError = ({ emptyTitle, error }: { readonly emptyTitle: string; readonly error: ApiError }) => {
  if (error.status === 401) {
    return null;
  }
  if (error.status === 404) {
    return <NotFoundNotice title={emptyTitle} />;
  }

  return <ApiErrorNotice error={error} forbiddenMessage="You do not have access to dag-healer data." />;
};

const Loading = () => (
  <HStack color="fg.muted" gap={2}>
    <Spinner size="sm" />
    <Text fontSize="sm">Loading…</Text>
  </HStack>
);

const Tile = ({ label, value }: { readonly label: string; readonly value: number }) => (
  <Stat.Root borderColor="border" borderRadius="md" borderWidth="1px" p={3}>
    <Stat.Label>{label}</Stat.Label>
    <Stat.ValueText>{value}</Stat.ValueText>
  </Stat.Root>
);

const FilterSelect = ({
  label,
  onChange,
  options,
  value,
}: {
  readonly label: string;
  readonly onChange: (value: string) => void;
  readonly options: ReadonlyArray<{ label: string; value: string }>;
  readonly value: string;
}) => (
  <NativeSelect.Root size="sm" width="auto">
    <NativeSelect.Field aria-label={label} onChange={(event) => onChange(event.target.value)} value={value}>
      {options.map((option) => (
        <option key={option.value} value={option.value}>
          {option.label}
        </option>
      ))}
    </NativeSelect.Field>
    <NativeSelect.Indicator />
  </NativeSelect.Root>
);

const DagsTable = ({ dags }: { readonly dags: Array<DagSummary> }) => (
  <Table.ScrollArea borderColor="border" borderRadius="md" borderWidth="1px" maxWidth="100%">
    <Table.Root aria-label="Monitored DAGs" size="sm" variant="line">
      <Table.Header>
        <Table.Row>
          <Table.ColumnHeader>DAG</Table.ColumnHeader>
          <Table.ColumnHeader>Contract</Table.ColumnHeader>
          <Table.ColumnHeader textAlign="end">Open incidents</Table.ColumnHeader>
          <Table.ColumnHeader>Last incident</Table.ColumnHeader>
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {dags.map((dag) => (
          <Table.Row data-dag-id={dag.dag_id} key={dag.dag_id}>
            <Table.Cell>
              <Link href={dagHref(dag.dag_id)}>{dag.dag_id}</Link>
              {dag.monitored ? undefined : (
                <Badge ms={2} variant="plain">
                  not monitored
                </Badge>
              )}
            </Table.Cell>
            <Table.Cell>
              <ContractStatusBadge status={dag.contract_status} />
            </Table.Cell>
            <Table.Cell textAlign="end">{dag.open_incidents}</Table.Cell>
            <Table.Cell>
              {dag.last_incident === null ? (
                <Text color="fg.muted" fontSize="sm">
                  none
                </Text>
              ) : (
                <HStack gap={2} wrap="wrap">
                  <Link
                    fontSize="sm"
                    href={taskInstanceHref(
                      dag.dag_id,
                      dag.last_incident.run_id,
                      dag.last_incident.task_id,
                      dag.last_incident.map_index,
                    )}
                  >
                    {formatDateTime(dag.last_incident.created_at)}
                  </Link>
                  <CategoryBadge category={dag.last_incident.category} />
                  <StatusBadge status={dag.last_incident.status} />
                </HStack>
              )}
            </Table.Cell>
          </Table.Row>
        ))}
      </Table.Body>
    </Table.Root>
  </Table.ScrollArea>
);

/** Top-level "Healer" page (nav destination): counts, incident timeline, monitored DAGs, contracts. */
export const HealerPage = () => {
  const { commandMs, refreshMs } = usePollingIntervals();
  const incidents = usePolledResource<IncidentListResponse>(
    "incidents",
    (signal) => listIncidents({ limit: INCIDENT_PAGE_LIMIT }, signal),
    refreshMs,
  );
  const dags = usePolledResource<DagListResponse>("dags", listDags, refreshMs);
  const contracts = usePolledResource<ContractListResponse>("contracts", listContracts, (data) =>
    data?.contracts.some((contract) => contract.pending_approval) ? commandMs : refreshMs,
  );

  const [statusFilter, setStatusFilter] = useState(ALL);
  const [categoryFilter, setCategoryFilter] = useState(ALL);
  const [dagFilter, setDagFilter] = useState(ALL);

  const allIncidents = useMemo(() => sortNewestFirst(incidents.data?.incidents ?? []), [incidents.data]);
  const counts = useMemo(() => countIncidents(allIncidents), [allIncidents]);
  const filtered = useMemo(
    () =>
      allIncidents.filter(
        (incident) =>
          (statusFilter === ALL || incident.status === statusFilter) &&
          (categoryFilter === ALL || (incident.category ?? UNCLASSIFIED) === categoryFilter) &&
          (dagFilter === ALL || incident.dag_id === dagFilter),
      ),
    [allIncidents, statusFilter, categoryFilter, dagFilter],
  );

  const statusOptions = useMemo(
    () => [
      { label: "All statuses", value: ALL },
      ...orderStatuses([...allIncidents.map((incident) => incident.status), statusFilter])
        .filter((status) => status !== ALL)
        .map((status) => ({ label: statusLabel(status), value: status })),
    ],
    [allIncidents, statusFilter],
  );
  const categoryOptions = [
    { label: "All categories", value: ALL },
    ...CATEGORIES.map((category) => ({ label: categoryLabel(category), value: category })),
    { label: "Unclassified", value: UNCLASSIFIED },
  ];
  const dagOptions = useMemo(() => {
    const ids = new Set([
      ...allIncidents.map((incident) => incident.dag_id),
      ...(dags.data?.dags ?? []).map((dag) => dag.dag_id),
    ]);

    if (dagFilter !== ALL) {
      ids.add(dagFilter);
    }

    return [{ label: "All DAGs", value: ALL }, ...[...ids].sort().map((id) => ({ label: id, value: id }))];
  }, [allIncidents, dags.data, dagFilter]);

  const sessionExpired = hasSessionExpired(incidents.error, dags.error, contracts.error);
  const total = incidents.data?.total ?? allIncidents.length;

  return (
    <Stack data-testid="healer-page" gap={8}>
      {sessionExpired ? <SessionExpiredBanner /> : undefined}

      <Stack gap={4}>
        <Box>
          <Heading as="h1" size="xl">
            Healer
          </Heading>
          <Text color="fg.muted">
            Task failures of monitored DAGs, classified by dag-healer as data, code or ambiguous.
          </Text>
        </Box>
        <SimpleGrid columns={{ base: 2, lg: 5, md: 3 }} data-testid="status-counts" gap={3}>
          <Tile label="Open" value={counts.open} />
          {STAT_TILES.map((tile) => (
            <Tile
              key={tile.label}
              label={tile.label}
              value={tile.statuses.reduce((sum, status) => sum + (counts.byStatus[status] ?? 0), 0)}
            />
          ))}
        </SimpleGrid>
        <HStack data-testid="category-counts" gap={2} wrap="wrap">
          {[...CATEGORIES, UNCLASSIFIED].map((category) => (
            <Badge
              colorPalette={categoryPalette(category === UNCLASSIFIED ? null : category)}
              data-category={category}
              key={category}
              size="md"
              variant="subtle"
            >
              {categoryLabel(category === UNCLASSIFIED ? null : category)}: {counts.byCategory[category] ?? 0}
            </Badge>
          ))}
        </HStack>
      </Stack>

      <Section
        actions={
          <HStack gap={2} wrap="wrap">
            <FilterSelect
              label="Filter by status"
              onChange={setStatusFilter}
              options={statusOptions}
              value={statusFilter}
            />
            <FilterSelect
              label="Filter by category"
              onChange={setCategoryFilter}
              options={categoryOptions}
              value={categoryFilter}
            />
            <FilterSelect
              label="Filter by DAG"
              onChange={setDagFilter}
              options={dagOptions}
              value={dagFilter}
            />
          </HStack>
        }
        title="Incidents"
      >
        {incidents.loading ? <Loading /> : undefined}
        {incidents.error === undefined ? undefined : (
          <SectionError emptyTitle="No incidents recorded yet." error={incidents.error} />
        )}
        {incidents.data === undefined ? undefined : allIncidents.length === 0 ? (
          <EmptyNotice title="No incidents recorded yet.">
            Failed tasks of monitored DAGs will show up here.
          </EmptyNotice>
        ) : filtered.length === 0 ? (
          <EmptyNotice title="No incident matches the filters." />
        ) : (
          <Stack gap={2}>
            <Text color="fg.muted" fontSize="sm">
              Showing {filtered.length} of {total} incidents
            </Text>
            <IncidentTimeline incidents={filtered} />
          </Stack>
        )}
      </Section>

      <Section title="Monitored DAGs">
        {dags.loading ? <Loading /> : undefined}
        {dags.error === undefined ? undefined : (
          <SectionError emptyTitle="No monitored DAGs." error={dags.error} />
        )}
        {dags.data === undefined ? undefined : dags.data.dags.length === 0 ? (
          <EmptyNotice title="No monitored DAGs.">
            List DAG ids (or glob patterns) under monitor.dags in healer.yaml.
          </EmptyNotice>
        ) : (
          <DagsTable dags={dags.data.dags} />
        )}
      </Section>

      <Section title="Contracts">
        {contracts.loading ? <Loading /> : undefined}
        {contracts.error === undefined ? undefined : (
          <SectionError emptyTitle="No data contracts yet." error={contracts.error} />
        )}
        {contracts.data === undefined ? undefined : contracts.data.contracts.length === 0 ? (
          <EmptyNotice title="No data contracts yet.">
            Run dag-healer init to infer draft contracts from your datasets.
          </EmptyNotice>
        ) : (
          <ContractsTable contracts={contracts.data.contracts} onQueued={contracts.refresh} />
        )}
      </Section>
    </Stack>
  );
};
