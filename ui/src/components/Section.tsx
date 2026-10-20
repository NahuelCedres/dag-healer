import { Box, Heading, HStack, type StackProps } from "@chakra-ui/react";
import type { ReactNode } from "react";

/** Titled block of a view; `actions` render on the right of the title. */
export const Section = ({
  actions,
  children,
  title,
  ...rest
}: {
  readonly actions?: ReactNode;
  readonly children: ReactNode;
  readonly title: ReactNode;
} & Omit<StackProps, "children" | "title">) => (
  <Box as="section" {...rest}>
    <HStack gap={3} justifyContent="space-between" mb={3} wrap="wrap">
      <Heading as="h3" size="md">
        {title}
      </Heading>
      {actions}
    </HStack>
    {children}
  </Box>
);
