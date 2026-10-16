# Product catalog

| | |
|---|---|
| Path | `Input\landing\reference\products\arrival_date=YYYY-MM-DD\` |
| Files | `product_catalog_<snapshot-date>_<delivery-ts>.csv` |
| Format | CSV: header row, comma delimiter, `"` quote |
| Delivery | A **full snapshot** of the catalog, delivered at 02:30 UTC; three snapshots in the dataset |
| Grain | One row per product per snapshot |
| Business key | `product_code` |

Hierarchy: product → family (`family_code`) → business unit.

## Columns

| Column | Type | Meaning |
|---|---|---|
| `product_code` | STRING | e.g. `LOGIN-SSO`; matches helpdesk `product` and call-centre `PRODUCT_LINE` |
| `product_name` | STRING | Display name |
| `family_code` | STRING | Product family |
| `family_name` | STRING | Family display name |
| `business_unit` | STRING | Owning business unit |
| `launch_date` | STRING (date) | `yyyy-MM-dd` |
| `retired_date` | STRING (date), empty when not retired | `yyyy-MM-dd` |
| `is_active` | STRING | `Y` / `N` |
