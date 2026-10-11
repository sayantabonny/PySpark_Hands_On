# Hands-on PySpark practice-data generator

Deterministic, labelled, deliberately messy input data for hands-on PySpark practice, plus the
ground truth used to check answers. Standard-library Python 3.10+ only.

The generator started as a copy of the SupportIQ test-data generator and has been changed for this
practice workspace in three ways:

- **One uniform encoding.** Every landing file is plain ASCII, so it is also valid UTF-8, and all
  text is English. There are no other languages or scripts, no emoji, no invisible Unicode control
  characters and no invalid byte sequences. One drifted call-centre CSV (scenario S27) still starts
  with a UTF-8 byte-order mark, because that tests header handling, not language.
- **Complex types (scenario S34)** for array, struct, map and nested-JSON practice (see the schemas below).
- **Data-modelling sources (S35–S37)** for CDC, SCD, star/snowflake and Data Vault practice:
  - a CRM customer-profile CDC feed
  - a product catalog (product → family → business unit)
  - an effective-dated agent roster
  - a billing account key map
  - early-arriving customers in tickets and calls

## Quick start (from `D:\Project_AI\PySpark_Hands_On`)

```bash
python -m pip install -e .\tools\datagen
python -m data_generator --profile small --out Input --clean
```

Outputs:

```
Input/landing/                      the practice inputs (helpdesk/, callcenter/, crm/)
Input/testdata/expected/small/      ground truth: manifest, batch expectations, record labels
```

Other commands:

```bash
python -m unittest discover -s tests -v                                    # run inside tools/datagen
python -m data_generator --profile small --out Input --clean --verify-determinism
```

## Sources and schemas

**Helpdesk** — `landing/helpdesk/tickets/arrival_date=YYYY-MM-DD/helpdesk_tickets_<ts>_<seq>.jsonl`.
Each export is one JSON object per line. Every data file has a `.ctl.json` control file with
`file_name`, `record_count`, `sha256`, `generated_at` and `schema_version`.

| Field | Type | Notes |
|---|---|---|
| ticket_id, created_at, updated_at, resolved_at, status, priority, channel, product, subject, description, assignee_id, category, schema_version | string | Timestamps are ISO-8601 with `Z` or an offset, optionally with milliseconds; or `yyyy-MM-dd HH:mm:ss` (UTC); or epoch milliseconds as text |
| reopen_count, csat | integer | |
| requester | struct<customer_id, email, locale> | |
| tags | array<string> | |
| comments | array<struct<comment_id, author struct<type, id>, created_at, public boolean, body, attachments array<struct<file_name, content_type, size_bytes long>>>> | Grows across ticket versions; the export keeps the most recent 20 comments |
| sla | struct<policy, targets array<struct<metric, target_minutes int, breached boolean>>, pause_windows array<array<string>>> | Not present for the social channel |
| custom_fields | object with a variable set of keys | Read it as map<string,string> |

**Call centre** — `landing/callcenter/tickets/arrival_date=.../callcenter_calls_<business_date>_<ts>.csv`,
RFC 4180 CSV with a header. Columns: `CALL_ID, CUST_NO, CALL_START, CALL_END, AGENT, QUEUE, PRIORITY_CD,
DISPOSITION, HANDLE_SECS, NOTES, PRODUCT_LINE, CSAT, IVR_JSON`. Call times are `dd/MM/yyyy HH:mm`
in Asia/Kolkata. `IVR_JSON` is JSON text inside a CSV column:
`{"entry", "menu_path": [string], "wait_secs": [int], "callback_offered": boolean, "transfers": [{"queue", "secs"}]}`.

**CRM** — `landing/crm/customers/arrival_date=.../crm_customers_<snapshot_date>_<ts>.csv`. This is a full daily
snapshot with columns `customer_id, customer_name, tier, region, industry, signup_date, is_active`.

**CRM customer-profile CDC** — `landing/crm/customer_changes/arrival_date=.../crm_customer_changes_<ts>_<seq>.jsonl`.
There is one initial snapshot file (op `r`), then one file per business day. Each line is an event with
`event_id, op (r|c|u|d), source_seq, commit_ts, customer_id, effective_from, changed_fields[]` and
`before`/`after` structs `{segment, account_manager, city, credit_limit, status}`. Apply events in `source_seq`
order after de-duplicating `event_id`. The answer keys are in
`testdata/expected/<profile>/warehouse/cdc_customer_profile_final.csv` and `cdc_customer_profile_scd2.csv`.

**Product catalog** — `landing/reference/products/...`, full snapshots with columns `product_code, product_name,
family_code, family_name, business_unit, launch_date, retired_date, is_active`.

**Agent roster** — `landing/reference/agents/...`, full-history extracts with columns `agent_id, agent_name, team,
site, role, effective_from, effective_to`. Dates are inclusive, and an empty `effective_to` means open.

**Billing account map** — `landing/billing/account_map/...`, full extracts with columns `billing_account_id,
customer_id, account_type, currency, payment_terms, valid_from, valid_to`.

## Common options

| Option | Meaning |
|---|---|
| `--profile small\|medium\|large` | volume profile (small ~40k helpdesk snapshots over 14 days, medium ~2M over 90, large ~10M over 180) |
| `--seed N`, `--anchor-date YYYY-MM-DD` | reproducibility knobs (same seed + config + Python minor version = byte-identical files) |
| `--batches B001,B003` | write only these batches (earlier ones are simulated, not written) |
| `--only-source helpdesk,crm` | restrict sources |
| `--config overrides.json` | override any setting (see `examples/overrides_example.json`) |
| `--landing-root`, `--expected-root` | write elsewhere |
| `--clean` | delete previous outputs of this profile first |
| `--no-validate` | skip the self-check |
| `--verify-determinism` | regenerate into a temp folder and compare every file hash |

Exit codes: 0 OK, 1 self-check or determinism failure, 2 configuration error.

The scenario catalogue is in `data_generator/scenarios/catalog.py`. Exact per-record outcomes are in
`Input/testdata/expected/<profile>/<batch>/record_labels.jsonl.gz`, which is answer-checking material and
should not be used as pipeline input. The terms used in the labels (for example "silver" for the
deduplicated current-state table, and the R-TKT/R-CUS rule ids) come from the generator's original design.
