---
pipeline: inventory_report
owner: "@ops"
reviewed: 2026-10-01
review_every: 1y
rule_prefix: INV
---

# inventory_report

Builds on the Organization Context (`healer/organization.md`).

The stock of each SKU in all the warehouses, every hour, for the reorders of operations.

## Source

- `data/incoming/inventory.csv`: the warehouse export of ops (ORG-2).

## Rules

- INV-1: Stock is counted per SKU in all the warehouses together. A SKU with fewer than 20 units
  is low stock, and its reorder brings it up to 150 units.
- INV-2: A renamed or new column of the warehouse export needs a word from ops before the report
  follows it. Until then, a failure that it causes waits for a person.
- INV-3 (overrides ORG-3): An empty quantity means that the warehouse has none of the SKU: it
  counts as 0.
