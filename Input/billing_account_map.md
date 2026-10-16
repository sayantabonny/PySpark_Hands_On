# Billing account map

| | |
|---|---|
| Path | `Input\landing\billing\account_map\arrival_date=YYYY-MM-DD\` |
| Files | `billing_account_map_<as-of-date>_<delivery-ts>.csv` |
| Format | CSV: header row, comma delimiter |
| Delivery | A **full extract** from the billing system as of the date in the file name; two extracts in the dataset |
| Grain | One row per billing account per validity interval |
| Business keys | `billing_account_id` (billing system) mapped to `customer_id` (CRM) |

Validity dates are **inclusive**. An empty `valid_to` means the mapping is open. A customer can have several billing accounts.

## Columns

| Column | Type | Meaning |
|---|---|---|
| `billing_account_id` | STRING | e.g. `BA00012341` |
| `customer_id` | STRING | CRM customer id (`C` + 7 digits); see [crm_customers.md](crm_customers.md) |
| `account_type` | STRING | `PRIMARY` / `SECONDARY` |
| `currency` | STRING | ISO currency code |
| `payment_terms` | STRING | `NET15`, `NET30`, `NET45`, `NET60` |
| `valid_from` | STRING (date) | `yyyy-MM-dd` |
| `valid_to` | STRING (date), empty when open | `yyyy-MM-dd` |
