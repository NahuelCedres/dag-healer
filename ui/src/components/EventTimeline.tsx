import { Badge, chakra, HStack, Text, Timeline } from "@chakra-ui/react";

import type { IncidentEvent } from "src/api";
import { formatDateTime, formatValue } from "src/format";

const MAX_DATA_CHARS = 400;

const dataText = (data: unknown): string | undefined => {
  if (data === null || data === undefined) {
    return undefined;
  }
  if (typeof data === "object" && Object.keys(data).length === 0) {
    return undefined;
  }
  const text = formatValue(data);

  return text.length > MAX_DATA_CHARS ? `${text.slice(0, MAX_DATA_CHARS - 1)}…` : text;
};

/** Timeline of an incident: status changes, decisions, alerts and human actions (oldest first). */
export const EventTimeline = ({ events }: { readonly events: Array<IncidentEvent> }) => (
  <Timeline.Root aria-label="Event timeline" size="sm" variant="subtle">
    {events.map((event, index) => {
      const data = dataText(event.data);

      return (
        <Timeline.Item data-testid="event-item" key={`${event.at}-${event.kind}-${index}`}>
          <Timeline.Connector>
            <Timeline.Separator />
            <Timeline.Indicator />
          </Timeline.Connector>
          <Timeline.Content gap={1} pb={4}>
            <Timeline.Title>
              <HStack gap={2} wrap="wrap">
                <chakra.time color="fg.muted" dateTime={event.at} fontSize="xs">
                  {formatDateTime(event.at)}
                </chakra.time>
                <Badge variant="surface">{event.kind}</Badge>
                <Text color="fg.muted" fontSize="xs">
                  by {event.actor}
                </Text>
              </HStack>
            </Timeline.Title>
            {event.message.length > 0 ? (
              <Timeline.Description color="fg" whiteSpace="pre-wrap">
                {event.message}
              </Timeline.Description>
            ) : undefined}
            {data === undefined ? undefined : (
              <Text color="fg.muted" fontFamily="mono" fontSize="xs" wordBreak="break-all">
                {data}
              </Text>
            )}
          </Timeline.Content>
        </Timeline.Item>
      );
    })}
  </Timeline.Root>
);
