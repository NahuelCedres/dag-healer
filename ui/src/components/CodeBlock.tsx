import { Box, type BoxProps } from "@chakra-ui/react";

/** Monospace, horizontally scrollable block for code, logs and pytest output. */
export const CodeBlock = ({
  children,
  label,
  ...rest
}: { readonly children: string; readonly label?: string } & Omit<BoxProps, "children">) => (
  <Box
    aria-label={label}
    as="pre"
    bg="bg.subtle"
    borderColor="border"
    borderRadius="md"
    borderWidth="1px"
    fontFamily="mono"
    fontSize="xs"
    lineHeight="1.6"
    maxHeight="32rem"
    overflow="auto"
    p={3}
    whiteSpace="pre"
    {...rest}
  >
    {children}
  </Box>
);
