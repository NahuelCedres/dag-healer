import { Badge, chakra, HStack, Link, Text, Timeline } from "@chakra-ui/react";

import type { IncidentSummary } from "src/api";
import { formatDateTime, oneLine, taskLabel } from "src/format";
import { dagHref, taskInstanceHref } from "src/links";
import { categoryPalette } from "src/palettes";

import { CategoryBadge, StatusBadge } from "./Badges";

/** One-line description of an incident: its diagnosis when the summary carries one, else the error. */
const headline = (incident: IncidentSummary): string =>
  oneLine(incident.diagnosis) || oneLine(incident.error_summary) || "No error message recorded.";

/** Incidents as a vertical timeline (callers sort them, newest first). */
export const IncidentTimeline = ({
  incidents,
  showDag = true,
}: {
  readonly incidents: Array<IncidentSummary>;
  readonly showDag?: boolean;
}) => (
  <Timeline.Root aria-label="Incident timeline" size="sm" variant="subtle">
    {incidents.map((incident) => (
      <Timeline.Item data-incident-id={incident.id} data-testid="incident-item" key={incident.id}>
        <Timeline.Connector>
          <Timeline.Separator />
          <Timeline.Indicator colorPalette={categoryPalette(incident.category)} />
        </Timeline.Connector>
        <Timeline.Content gap={1} pb={5}>
          <Timeline.Title>
            <HStack gap={2} wrap="wrap">
              <chakra.time color="fg.muted" dateTime={incident.created_at} fontSize="xs">
                {formatDateTime(incident.created_at)}
              </chakra.time>
              {showDag ? (
                <Link fontWeight="semibold" href={dagHref(incident.dag_id)}>
                  {incident.dag_id}
                </Link>
              ) : undefined}
              <Text as="span" fontFamily="mono" fontSize="sm">
                {taskLabel(incident.task_id, incident.map_index)}
              </Text>
              <CategoryBadge category={incident.category} />
              <StatusBadge status={incident.status} />
              {incident.occurrences > 1 ? (
                <Badge title="Occurrences of the same failure" variant="plain">
                  x{incident.occurrences}
                </Badge>
              ) : undefined}
            </HStack>
          </Timeline.Title>
          <Timeline.Description color="fg" lineClamp={2}>
            {headline(incident)}
          </Timeline.Description>
          <HStack fontSize="sm" gap={4}>
            <Link
              href={taskInstanceHref(incident.dag_id, incident.run_id, incident.task_id, incident.map_index)}
              variant="underline"
            >
              Open incident
            </Link>
            {incident.pr_url === null ? undefined : (
              <Link href={incident.pr_url} rel="noopener noreferrer" target="_blank">
                Pull request
              </Link>
            )}
            <Text color="fg.subtle" fontSize="xs">
              run {incident.run_id} · try {incident.try_number}
            </Text>
          </HStack>
        </Timeline.Content>
      </Timeline.Item>
    ))}
  </Timeline.Root>
);
