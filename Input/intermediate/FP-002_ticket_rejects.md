# FP_002_ticket_rejects (intermediate table)

| | |
|---|---|
| Location | `D:\Project_AI\PySpark_Hands_On\Output\focused\FP-002_ticket_rejects` |
| Format | Delta, unpartitioned. Read it by path. |
| Table identifier (when registered) | `FP_002_ticket_rejects` |
| Produced by | FP-002, using the interviewer-corrected code (validated against the answer key) |
| Source data | Same input as [FP-002_ticket_current.md](FP-002_ticket_current.md) |
| Grain | One row per rejected input line |
| Rows | 185: `MALFORMED_RECORD` 67, `MISSING_TICKET_ID` 81, `INVALID_UPDATED_AT` 37 |
| Delta history | Versions 0–2: the candidate's attempts. **Version 3: the corrected, current version.** |

## Columns

| Column | Type | Meaning |
|---|---|---|
| `_source_file` | STRING | Full path of the source file |
| `ticket_id` | STRING, nullable | As received (not trimmed); null for malformed lines |
| `updated_at_raw` | STRING, nullable | `updated_at` as received |
| `reject_reason` | STRING | The first failing rule, checked in this order: `MALFORMED_RECORD`, `MISSING_TICKET_ID`, `INVALID_UPDATED_AT` |

Reconciliation with the input: 13,004 input lines = 12,819 valid + 185 rejected.
