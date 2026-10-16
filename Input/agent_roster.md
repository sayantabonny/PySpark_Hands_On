# Agent roster

| | |
|---|---|
| Path | `Input\landing\reference\agents\arrival_date=YYYY-MM-DD\` |
| Files | `agent_roster_<as-of-date>_<delivery-ts>.csv` |
| Format | CSV: header row, comma delimiter |
| Delivery | A **full-history extract** as of the date in the file name; two extracts in the dataset. Each extract replaces the previous one completely. |
| Grain | One row per agent per validity interval |
| Business key | `agent_id`; one version is identified by (`agent_id`, `effective_from`) |

Validity dates are **inclusive** on both ends. An empty `effective_to` means the row is still open as of the extract date.

## Columns

| Column | Type | Meaning |
|---|---|---|
| `agent_id` | STRING | `AGT-0001`…; matches helpdesk `assignee_id` and call-centre `AGENT` |
| `agent_name` | STRING | Name |
| `team` | STRING | Helpdesk teams `HD-*`, call-centre teams `CC-*` |
| `site` | STRING | Work location |
| `role` | STRING | `AGENT`, `SENIOR_AGENT`, `TEAM_LEAD` |
| `effective_from` | STRING (date) | `yyyy-MM-dd` |
| `effective_to` | STRING (date), empty when open | `yyyy-MM-dd` |
