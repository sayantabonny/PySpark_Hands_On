# CRM customer-profile change feed (CDC)

| | |
|---|---|
| Path | `Input\landing\crm\customer_changes\arrival_date=YYYY-MM-DD\` |
| Files | `crm_customer_changes_<delivery-ts>_<seq>.jsonl`: one change event per line |
| Delivery | First an initial snapshot file (2026-09-02 00:30 UTC, all events `op = r`). Then one file per business day, delivered the next day at 03:00 UTC. |
| Grain | One row per change event |
| Event key | `event_id` |
| Entity key | `customer_id` |

The profile system is separate from the CRM snapshot extract and keeps its own attributes for a subset of customers.

## Columns

| Column | Type | Meaning |
|---|---|---|
| `event_id` | STRING | Unique id of the change event |
| `op` | STRING | `r` = initial snapshot row, `c` = create, `u` = update, `d` = delete (hard delete) |
| `source_seq` | BIGINT | Commit order in the source system; a higher value committed later |
| `commit_ts` | STRING (timestamp) | System time of the commit, ISO-8601 UTC with milliseconds |
| `customer_id` | STRING | Profile key |
| `effective_from` | STRING (date) | Business date from which the change applies, `yyyy-MM-dd` |
| `changed_fields` | ARRAY<STRING> | Attribute names changed by this event |
| `before` | STRUCT<segment STRING, account_manager STRING, city STRING, credit_limit INT, status STRING>, nullable | Image before the change (null for `r` and `c`) |
| `after` | same STRUCT, nullable | Image after the change (null for `d`) |

Attribute vocabularies:
- `segment`: `SMB`, `MID_MARKET`, `ENTERPRISE`
- `account_manager`: `AM-001`…`AM-040`, nullable (unassigned)
- `status`: `ACTIVE`, `ON_HOLD`, `CHURN_RISK`
