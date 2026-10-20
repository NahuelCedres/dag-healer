import { Badge, Box, HStack, Table, Text } from "@chakra-ui/react";

import type { MaskedSample } from "src/api";
import { formatValue } from "src/format";
import { MASKED_LABEL } from "src/messages";

const cellValue = (row: MaskedSample["rows"][number], column: string, index: number): unknown =>
  Array.isArray(row) ? row[index] : row[column];

/** Columns of a sample: the declared ones, else the keys of the first record. */
const sampleColumns = (sample: MaskedSample): Array<string> => {
  if (sample.columns.length > 0) {
    return sample.columns;
  }
  const first = sample.rows[0];

  return first !== undefined && !Array.isArray(first) ? Object.keys(first) : [];
};

/** The masked rows stored with an incident (masked server-side before they were ever persisted). */
export const MaskedSampleTable = ({ sample }: { readonly sample: MaskedSample }) => {
  const columns = sampleColumns(sample);

  return (
    <Box>
      <HStack gap={2} mb={2} wrap="wrap">
        <Text fontSize="sm">
          Dataset{" "}
          <Text as="span" fontFamily="mono">
            {sample.dataset}
          </Text>
        </Text>
        <Badge data-testid="masked-label" variant="surface">
          {MASKED_LABEL}
        </Badge>
      </HStack>
      {sample.rows.length === 0 ? (
        <Text color="fg.muted" fontSize="sm">
          The sample has no rows.
        </Text>
      ) : (
        <Table.ScrollArea borderColor="border" borderRadius="md" borderWidth="1px" maxWidth="100%">
          <Table.Root aria-label="Masked data sample" size="sm" variant="line">
            <Table.Header>
              <Table.Row>
                {columns.map((column) => (
                  <Table.ColumnHeader fontFamily="mono" key={column}>
                    {column}
                  </Table.ColumnHeader>
                ))}
              </Table.Row>
            </Table.Header>
            <Table.Body>
              {sample.rows.map((row, rowIndex) => (
                // Sample rows are positional and never reordered.
                <Table.Row key={rowIndex}>
                  {columns.map((column, columnIndex) => {
                    const value = cellValue(row, column, columnIndex);

                    return (
                      <Table.Cell
                        color={value === null || value === undefined ? "fg.subtle" : undefined}
                        fontFamily="mono"
                        key={column}
                        whiteSpace="nowrap"
                      >
                        {formatValue(value)}
                      </Table.Cell>
                    );
                  })}
                </Table.Row>
              ))}
            </Table.Body>
          </Table.Root>
        </Table.ScrollArea>
      )}
    </Box>
  );
};
