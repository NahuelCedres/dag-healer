import { Box, Flex, HStack, Text } from "@chakra-ui/react";
import { useMemo } from "react";

import { type DiffLine, type DiffLineKind, diffStats, parseDiff } from "src/diff";

const LINE_STYLES: Record<DiffLineKind, { bg?: string; color?: string; fontWeight?: string }> = {
  add: { bg: "bg.success", color: "fg.success" },
  context: {},
  del: { bg: "bg.error", color: "fg.error" },
  file: { bg: "bg.muted", fontWeight: "semibold" },
  hunk: { bg: "bg.info", color: "fg.info" },
  meta: { color: "fg.muted" },
};

const Gutter = ({ value }: { readonly value: number | undefined }) => (
  <Box aria-hidden color="fg.subtle" flexShrink={0} pe={2} textAlign="right" userSelect="none" width="3.5em">
    {value ?? ""}
  </Box>
);

const Line = ({ line }: { readonly line: DiffLine }) => (
  <Flex data-kind={line.kind} minWidth="fit-content" {...LINE_STYLES[line.kind]}>
    <Gutter value={line.oldNumber} />
    <Gutter value={line.newNumber} />
    <Box as="span" pe={3} whiteSpace="pre">
      {line.text.length > 0 ? line.text : " "}
    </Box>
  </Flex>
);

/** Unified diff with colored added/removed lines, old/new line numbers and a files/lines summary. */
export const DiffView = ({ diff }: { readonly diff: string | null | undefined }) => {
  const lines = useMemo(() => parseDiff(diff), [diff]);
  const stats = useMemo(() => diffStats(diff), [diff]);

  if (lines.length === 0) {
    return (
      <Text color="fg.muted" fontSize="sm">
        No changes.
      </Text>
    );
  }

  return (
    <Box>
      <HStack color="fg.muted" fontSize="sm" gap={3} mb={2}>
        <Text>
          {stats.files.length} {stats.files.length === 1 ? "file" : "files"} changed
        </Text>
        <Text color="fg.success">+{stats.additions}</Text>
        <Text color="fg.error">-{stats.deletions}</Text>
      </HStack>
      <Box
        aria-label="Diff"
        borderColor="border"
        borderRadius="md"
        borderWidth="1px"
        data-testid="diff-view"
        fontFamily="mono"
        fontSize="xs"
        lineHeight="1.6"
        maxHeight="32rem"
        overflow="auto"
        role="region"
      >
        {lines.map((line, index) => (
          // Lines have no identity of their own; the diff is replaced as a whole.
          <Line key={index} line={line} />
        ))}
      </Box>
    </Box>
  );
};
