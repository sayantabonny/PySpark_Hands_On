# FP_002_ticket_current (intermediate table)

| | |
|---|---|
| Location | `D:\Project_AI\PySpark_Hands_On\Output\focused\FP-002_ticket_current` |
| Format | Delta, unpartitioned. Read it by path; the table name was registered only in an in-memory catalog session. |
| Table identifier (when registered) | `FP_002_ticket_current` |
| Produced by | FP-002, using the interviewer-corrected code (validated against the answer key) |
| Source data | Helpdesk ticket files under `Input\landing\helpdesk\tickets\`, folders `arrival_date=2026-09-02` … `2026-09-08` (41 `*.jsonl` files, 13,004 lines) |
| Grain | One row per ticket: its current state within that window |
| Business key | `ticket_id` (trimmed; unique) |
| Rows | 4,386 |
| Delta history | Versions 0–2: the candidate's attempts. **Version 3: the corrected, current version.** Use the latest version. |

## Columns

| Column | Type | Meaning |
|---|---|---|
| `ticket_id` | STRING | Trimmed ticket key, never blank |
| `status` | STRING | As received in the winning record |
| `priority` | STRING | As received in the winning record |
| `updated_at_raw` | STRING | `updated_at` as received |
| `updated_at_utc` | TIMESTAMP | The instant of `updated_at` (all three source encodings parsed as UTC instants) |
| `_source_file` | STRING | Full path of the file the winning record came from |
| `version_count` | INT | Number of valid records received for the ticket in the window (sum = 12,819; max = 42) |

## Rules that produced it

- Records were valid only when they were parseable JSON, had a non-blank `ticket_id` after trimming, and had an `updated_at` that parsed under the contract. The others are in [FP-002_ticket_rejects.md](FP-002_ticket_rejects.md).
- Winner per key: the latest `updated_at_utc`, then the later `_source_file` (delivery order), then a content hash as a deterministic final tie-break. No fields were merged across versions.
