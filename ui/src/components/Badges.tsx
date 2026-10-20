import { Badge, type BadgeProps } from "@chakra-ui/react";

import { categoryLabel, contractStatusLabel, statusLabel } from "src/format";
import { categoryPalette, contractPalette, statusPalette } from "src/palettes";

type Rest = Omit<BadgeProps, "children" | "colorPalette">;

/** Data (orange), code (blue) or ambiguous (purple); gray while unclassified. */
export const CategoryBadge = ({
  category,
  ...rest
}: { readonly category: string | null | undefined } & Rest) => (
  <Badge colorPalette={categoryPalette(category)} data-category={category ?? "none"} {...rest}>
    {categoryLabel(category)}
  </Badge>
);

/** Incident status badge. */
export const StatusBadge = ({ status, ...rest }: { readonly status: string } & Rest) => (
  <Badge colorPalette={statusPalette(status)} data-status={status} variant="outline" {...rest}>
    {statusLabel(status)}
  </Badge>
);

/** Contract status badge (approved, draft, stale, missing). */
export const ContractStatusBadge = ({ status, ...rest }: { readonly status: string } & Rest) => (
  <Badge colorPalette={contractPalette(status)} data-contract-status={status} variant="surface" {...rest}>
    {contractStatusLabel(status)}
  </Badge>
);
