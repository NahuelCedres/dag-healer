---
pipeline: sales_daily_etl
owner: "@data-eng"
reviewed: 2026-10-01
review_every: 1y
rule_prefix: SAL
---

# sales_daily_etl

Builds on the Organization Context (`healer/organization.md`).

The daily revenue summary per currency, from the point-of-sale export, for finance.

## Source

- `data/incoming/sales.csv`: the point-of-sale export of the store systems team (ORG-2).

## Rules

- SAL-1: `amount` is the order total in the order's currency. The summary keeps the revenue of
  each currency next to its value in USD.
- SAL-2: Refunds (an amount of 0 or less) are left out of this summary: finance handles them.
- SAL-3: The point-of-sale export renames or drops a column only in a release announced in #pos.
  An unannounced change is a problem of the export, which its owners fix (ORG-2).
- SAL-4: An order is unique by `order_id`. The export may send an order twice; the last copy wins.
