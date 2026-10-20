import { Box, Code, DataList, Table, Text } from "@chakra-ui/react";

import type { ViolatedRule, Violation } from "src/api";
import { formatValue } from "src/format";

const rows = (indexes: Array<number> | undefined): string =>
  indexes === undefined || indexes.length === 0 ? "" : indexes.join(", ");

/** Contract violations found in the current dataset files. */
export const ViolationsTable = ({ violations }: { readonly violations: Array<Violation> }) => (
  <Table.ScrollArea borderColor="border" borderRadius="md" borderWidth="1px" maxWidth="100%">
    <Table.Root aria-label="Contract violations" size="sm" variant="line">
      <Table.Header>
        <Table.Row>
          <Table.ColumnHeader>Rule</Table.ColumnHeader>
          <Table.ColumnHeader>Dataset</Table.ColumnHeader>
          <Table.ColumnHeader>Column</Table.ColumnHeader>
          <Table.ColumnHeader>Message</Table.ColumnHeader>
          <Table.ColumnHeader>Expected</Table.ColumnHeader>
          <Table.ColumnHeader>Actual</Table.ColumnHeader>
          <Table.ColumnHeader>Rows</Table.ColumnHeader>
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {violations.map((violation, index) => (
          <Table.Row key={`${violation.rule}-${violation.dataset ?? ""}-${violation.column ?? ""}-${index}`}>
            <Table.Cell>
              <Code size="sm">{violation.rule}</Code>
            </Table.Cell>
            <Table.Cell>{violation.dataset ?? ""}</Table.Cell>
            <Table.Cell fontFamily="mono">{violation.column ?? ""}</Table.Cell>
            <Table.Cell minWidth="16rem">{violation.message ?? ""}</Table.Cell>
            <Table.Cell fontFamily="mono">
              {violation.expected === undefined ? "" : formatValue(violation.expected)}
            </Table.Cell>
            <Table.Cell fontFamily="mono">
              {violation.actual === undefined ? "" : formatValue(violation.actual)}
            </Table.Cell>
            <Table.Cell fontFamily="mono">{rows(violation.row_indexes)}</Table.Cell>
          </Table.Row>
        ))}
      </Table.Body>
    </Table.Root>
  </Table.ScrollArea>
);

const KNOWN_KEYS = new Set(["actual", "column", "dataset", "expected", "message", "row_indexes", "rule"]);

/** The rule picked as the main cause of a data incident. */
export const ViolatedRuleCard = ({ rule }: { readonly rule: ViolatedRule }) => {
  const extra = Object.entries(rule).filter(([key, value]) => !KNOWN_KEYS.has(key) && value !== null);

  return (
    <Box
      bg="bg.subtle"
      borderColor="border"
      borderRadius="md"
      borderWidth="1px"
      data-testid="violated-rule"
      p={3}
    >
      <DataList.Root orientation="horizontal" size="sm">
        {rule.rule === undefined ? undefined : (
          <DataList.Item>
            <DataList.ItemLabel>Rule</DataList.ItemLabel>
            <DataList.ItemValue>
              <Code size="sm">{String(rule.rule)}</Code>
            </DataList.ItemValue>
          </DataList.Item>
        )}
        {rule.dataset === undefined ? undefined : (
          <DataList.Item>
            <DataList.ItemLabel>Dataset</DataList.ItemLabel>
            <DataList.ItemValue>{String(rule.dataset)}</DataList.ItemValue>
          </DataList.Item>
        )}
        {rule.column === undefined || rule.column === null ? undefined : (
          <DataList.Item>
            <DataList.ItemLabel>Column</DataList.ItemLabel>
            <DataList.ItemValue fontFamily="mono">{String(rule.column)}</DataList.ItemValue>
          </DataList.Item>
        )}
        {rule.expected === undefined ? undefined : (
          <DataList.Item>
            <DataList.ItemLabel>Expected</DataList.ItemLabel>
            <DataList.ItemValue fontFamily="mono">{formatValue(rule.expected)}</DataList.ItemValue>
          </DataList.Item>
        )}
        {rule.actual === undefined ? undefined : (
          <DataList.Item>
            <DataList.ItemLabel>Actual</DataList.ItemLabel>
            <DataList.ItemValue fontFamily="mono">{formatValue(rule.actual)}</DataList.ItemValue>
          </DataList.Item>
        )}
        {extra.map(([key, value]) => (
          <DataList.Item key={key}>
            <DataList.ItemLabel>{key}</DataList.ItemLabel>
            <DataList.ItemValue fontFamily="mono">{formatValue(value)}</DataList.ItemValue>
          </DataList.Item>
        ))}
      </DataList.Root>
      {rule.message === undefined ? undefined : (
        <Text fontSize="sm" mt={2}>
          {String(rule.message)}
        </Text>
      )}
    </Box>
  );
};
