# FP_001_queue_action (intermediate table)

| | |
|---|---|
| Location | `D:\Project_AI\PySpark_Hands_On\Output\focused\FP-001_queue_action` |
| Format | Delta, unpartitioned. Read it by path: `spark.read.format("delta").load(...)`; the table name was registered only in an in-memory catalog session. |
| Table identifier (when registered) | `FP_001_queue_action` |
| Produced by | FP-001 (helpdesk queue classification) |
| Source data | Helpdesk ticket files of `arrival_date=2026-06-18` from the **earlier** SupportIQ dataset (not in the current `Input`) |
| Grain | One row per input JSON line (ticket version), no de-duplication |
| Rows | 10,983 |
| Delta history | Version 0 and version 1 (both full overwrites with the same content) |

## Columns

| Column | Type | Meaning |
|---|---|---|
| `ticket_id` | STRING, nullable | As received |
| `status` | STRING, nullable | As received (not normalised) |
| `priority` | STRING, nullable | As received (not normalised) |
| `queue_action` | STRING, not null | `CLOSED_NO_FOLLOW_UP`, `WAITING_FOR_CUSTOMER`, `ESCALATE`, `STANDARD_QUEUE`, `NEEDS_REVIEW` |

## Rules that produced `queue_action`

Applied in order to trimmed, upper-cased `status` and `priority`:
1. `SOLVED`, `CLOSED` → `CLOSED_NO_FOLLOW_UP`
2. `PENDING`, `ON-HOLD` → `WAITING_FOR_CUSTOMER`
3. `NEW`, `OPEN` with `URGENT`, `HIGH` → `ESCALATE`
4. `NEW`, `OPEN` with `NORMAL`, `LOW` → `STANDARD_QUEUE`
5. Anything else, including missing values → `NEEDS_REVIEW`
