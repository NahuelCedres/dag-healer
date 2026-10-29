---
pipeline: customers_sync
owner: "@crm-team"
reviewed: 2026-10-01
review_every: 1y
rule_prefix: CUS
---

# customers_sync

Builds on the Organization Context (`healer/organization.md`).

The customers dimension, rebuilt every night from the CRM export.

## Source

- `data/incoming/customers.csv`: the CRM export of crm-team (ORG-2).

## Rules

- CUS-1: `signup_date` is the day the customer signed up. The CRM export sends it as a date
  (2025-01-14) or as an ISO-8601 timestamp in UTC (2026-10-02T14:03:00Z). Both are valid, and
  the signup date is the date part.
- CUS-2: `customer_id` is unique in the customers dimension; the last row of the export wins.
- CUS-3: `full_name`, `email` and `phone` are personal data (ORG-1): masked in samples and
  alerts, and never used in tests.
