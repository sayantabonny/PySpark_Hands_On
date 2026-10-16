# CRM customer snapshots

| | |
|---|---|
| Path | `Input\landing\crm\customers\arrival_date=YYYY-MM-DD\` |
| Files | `crm_customers_<snapshot-date>_<delivery-ts>.csv` |
| Format | CSV (RFC 4180): header row, comma delimiter, `"` quote and escape |
| Delivery | A **full snapshot** of all customers as of the snapshot date, delivered the next day at 02:00 UTC. A snapshot date can be delivered more than once. |
| Grain | One row per customer per snapshot |
| Business key | `customer_id` (format `C` + 7 digits) |

A customer missing from a complete snapshot is no longer in CRM.

## Columns

| Column | Type | Meaning |
|---|---|---|
| `customer_id` | STRING | Customer id |
| `customer_name` | STRING | Legal name, which may contain commas and quotes |
| `tier` | STRING | `ENTERPRISE`, `BUSINESS`, `STANDARD` |
| `region` | STRING | `APAC`, `EMEA`, `NA`, `LATAM` |
| `industry` | STRING | Industry name |
| `signup_date` | STRING (date) | `yyyy-MM-dd` |
| `is_active` | STRING | `Y` / `N` |

Related: tickets (`requester.customer_id`), calls (`CUST_NO`), the profile change feed and the billing account map all reference `customer_id`.
