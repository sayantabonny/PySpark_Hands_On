# Helpdesk ticket snapshots

| | |
|---|---|
| Path | `Input\landing\helpdesk\tickets\arrival_date=YYYY-MM-DD\` |
| Data files | `helpdesk_tickets_<delivery-ts>_<seq>.jsonl`: one JSON object per line |
| Control files | `<same name>.ctl.json`: one per data file |
| Delivery | An export every 4 hours (00:00, 04:00, … 20:00 UTC) holding the latest state of every ticket that changed in the previous 4 hours; large exports are split into several `_<seq>` files. Some later folders also hold re-deliveries and replays. |
| Grain | One row per ticket **version** (snapshot). The same `ticket_id` appears again whenever the ticket changes. |
| Business key | `ticket_id` |

## Columns

| Column | Type | Meaning |
|---|---|---|
| `ticket_id` | STRING | Ticket identifier, e.g. `HD-10000053` |
| `created_at` | STRING (timestamp) | Ticket creation time |
| `updated_at` | STRING (timestamp) | Time of the change captured by this version |
| `resolved_at` | STRING (timestamp), nullable | Resolution time, when resolved |
| `status` | STRING | Source vocabulary: `new`, `open`, `pending`, `on-hold`, `solved`, `closed` |
| `priority` | STRING | Source vocabulary: `urgent`, `high`, `normal`, `low` |
| `channel` | STRING | `email`, `chat`, `web`, `social`, `api` |
| `product` | STRING | Product code; see [product_catalog.md](product_catalog.md) |
| `subject` | STRING | Short title |
| `description` | STRING | Free text, possibly multi-line |
| `requester` | STRUCT<customer_id STRING, email STRING, locale STRING> | `customer_id` refers to CRM; see [crm_customers.md](crm_customers.md) |
| `assignee_id` | STRING, nullable | Agent id; see [agent_roster.md](agent_roster.md) |
| `tags` | ARRAY<STRING> | Labels |
| `category` | STRING | Agent-selected category |
| `reopen_count` | INT | Times the ticket was reopened |
| `csat` | INT, nullable | Satisfaction score 1–5 |
| `comments` | ARRAY<STRUCT<comment_id STRING, author STRUCT<type STRING, id STRING>, created_at STRING, public BOOLEAN, body STRING, attachments ARRAY<STRUCT<file_name STRING, content_type STRING, size_bytes BIGINT>>>> | Conversation history, most recent 20 comments. `author.type` is `customer`, `agent` or `system` |
| `sla` | STRUCT<policy STRING, targets ARRAY<STRUCT<metric STRING, target_minutes INT, breached BOOLEAN>>, pause_windows ARRAY<ARRAY<STRING>>> | SLA policy, its targets (`first_response`, `resolution`) and pause intervals `[start, end]`. Not present for the `social` channel |
| `custom_fields` | JSON object with a variable set of keys | Read it as MAP<STRING, STRING> |
| `schema_version` | STRING | Record layout version |

## Timestamp contract

`created_at`, `updated_at`, `resolved_at` and the timestamps inside `comments` and `sla` arrive in one of these encodings:
- ISO-8601 with `Z` or a numeric offset, optional milliseconds: `2026-09-02T03:28:59Z`, `2026-09-02T09:58:59+05:30`, `2026-09-02T03:28:59.000Z`
- `yyyy-MM-dd HH:mm:ss`, meaning UTC
- Unix epoch milliseconds as a digit string

## Control file (`.ctl.json`)

`{"file_name", "record_count", "sha256", "generated_at", "schema_version"}`. These describe the sibling data file. When reading the data, select `*.jsonl` only.
