/** Color palettes of badges and indicators: the only place the UI picks colors (via `colorPalette`). */
import type { Category } from "src/api";

export const CATEGORY_PALETTES: Record<Category, string> = {
  ambiguous: "purple",
  code: "blue",
  data: "orange",
};

const STATUS_PALETTES: Record<string, string> = {
  error: "red",
  repair_failed: "red",
  repaired: "green",
  reported: "orange",
  waiting_human: "yellow",
};

const CONTRACT_PALETTES: Record<string, string> = {
  approved: "green",
  stale: "yellow",
};

export const categoryPalette = (category: string | null | undefined): string =>
  category === null || category === undefined ? "gray" : (CATEGORY_PALETTES[category as Category] ?? "gray");

export const statusPalette = (status: string): string => STATUS_PALETTES[status] ?? "gray";

export const contractPalette = (status: string): string => CONTRACT_PALETTES[status] ?? "gray";
