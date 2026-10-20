import { EmptyState } from "@chakra-ui/react";
import type { ReactNode } from "react";

/** Neutral empty state ("nothing here yet"). */
export const EmptyNotice = ({
  children,
  title,
}: {
  readonly children?: ReactNode;
  readonly title: string;
}) => (
  <EmptyState.Root borderColor="border" borderRadius="md" borderStyle="dashed" borderWidth="1px" size="sm">
    <EmptyState.Content>
      <EmptyState.Title>{title}</EmptyState.Title>
      {children === undefined ? undefined : <EmptyState.Description>{children}</EmptyState.Description>}
    </EmptyState.Content>
  </EmptyState.Root>
);
