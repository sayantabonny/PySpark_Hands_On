# Call-centre calls

| | |
|---|---|
| Path | `Input\landing\callcenter\tickets\arrival_date=YYYY-MM-DD\` |
| Files | `callcenter_calls_<business-date>_<delivery-ts>.csv` |
| Format | CSV (RFC 4180): header row, comma delimiter, `"` quote and escape (doubled quote), CRLF line ends, multi-line quoted values possible |
| Delivery | One file per business day, delivered the next day at 01:30 UTC. Later folders also contain correction and re-export files. |
| Grain | One row per call |
| Business key | `CALL_ID` |

Map columns by **header name**, not by position.

## Columns

| Column | Type | Meaning |
|---|---|---|
| `CALL_ID` | STRING | e.g. `CC20260902000123` |
| `CUST_NO` | STRING, nullable | Customer id; see [crm_customers.md](crm_customers.md) |
| `CALL_START` | STRING (timestamp) | `dd/MM/yyyy HH:mm`, local time Asia/Kolkata (UTC+05:30) |
| `CALL_END` | STRING (timestamp), nullable | Same format |
| `AGENT` | STRING | Agent id; see [agent_roster.md](agent_roster.md) |
| `QUEUE` | STRING | `ACCESS`, `BILLING`, `TECH`, `GENERAL` |
| `PRIORITY_CD` | STRING | `P1`–`P4` |
| `DISPOSITION` | STRING | `RESOLVED`, `ESCALATED`, `CALLBACK`, `ABANDONED` |
| `HANDLE_SECS` | STRING (integer) | Handle time in seconds |
| `NOTES` | STRING | Agent notes, free text |
| `PRODUCT_LINE` | STRING | Product code; see [product_catalog.md](product_catalog.md) |
| `CSAT` | STRING (integer), nullable | Survey score 1–5 |
| `IVR_JSON` | STRING (JSON text), nullable | IVR journey, see below |

## `IVR_JSON` structure

```text
STRUCT<
  entry            STRING,
  menu_path        ARRAY<STRING>,
  wait_secs        ARRAY<INT>,          -- one value per menu_path element
  callback_offered BOOLEAN,
  transfers        ARRAY<STRUCT<queue STRING, secs INT>>
>
```
