---
owner: "@data-eng"
reviewed: 2026-10-01
review_every: 1y
---

# Organization context

The daily batch jobs of the online shop: sales, CRM customers and inventory. dag-healer reads
this file before every decision, in every pipeline. Each pipeline's ETL Information in
`healer/etl/` adds what is specific to it, and can replace a rule here by naming its ID.

## What we do

The shop sells to customers in several countries and currencies. Finance reads the daily sales
summary every morning, and operations reorders stock from the inventory report.

## Systems and owners

- The point-of-sale export (`data/incoming/sales.csv`): the store systems team, Slack #pos.
- The CRM export (`data/incoming/customers.csv`): crm-team, Slack #crm.
- The warehouse export (`data/incoming/inventory.csv`): ops, Slack #ops.
- The pipelines and their outputs: data-eng, Slack #data-eng. On-call: #data-oncall.

## Glossary

- **SKU**: a stock keeping unit, one product. **Low stock**: fewer than 20 units of a SKU in all
  the warehouses together.

## Rules for every pipeline

- ORG-1: Customer names, e-mail addresses and phone numbers are personal data. They are masked
  everywhere: samples, alerts, logs, tests and pull requests. Tests use made-up values.
- ORG-2: dag-healer alerts the on-call channel. Only the team that owns an export asks for it to
  be fixed or sent again.
- ORG-3: A wrong number in a report is worse than a late one. Invalid rows fail the task with a
  clear message; they are never dropped, defaulted or fixed silently.
- ORG-4: A fix is the smallest change that makes the failing test pass. Files from before a
  change must still load, because old dates are run again.
- ORG-5: A fix never changes contracts, schedules, retries or alert settings.
