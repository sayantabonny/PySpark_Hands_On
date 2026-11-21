# PySpark / Databricks Hands-on Practice Guide

## Purpose and candidate profile

Use this guide to coach a candidate with 8–10 years of experience through hands-on PySpark exercises on deterministic synthetic data.

- **Primary Objective:** Build confidence for a hands-on PySpark Databricks interview.
- **Secondary Objective:** Practice concise PySpark coding steps, reason about architecture and data correctness, and use Spark UI evidence to understand execution. Look up syntax as needed; no memorization or scoring programme.

This practice is standalone and completely separate from the SupportIQ and IncidentIQ projects. The support-desk dataset is practice material only. Do not frame exercises as project modules, and do not prepare project claims.

The candidate has Windows local Spark with winutils and Delta support, plus a Databricks environment for operations that need Databricks services.

## Workspace

Root: `D:\Project_AI\PySpark_Hands_On`

| Path | Content |
|---|---|
| `tools\datagen\` | Data generator (Python package `data_generator`, distribution `handson-datagen`) |
| `Input\landing\{helpdesk,callcenter,crm,reference,billing}\` | Practice input data |
| `Input\*.md`, `Input\intermediate\*.md` | Candidate-facing schema files: one per source (contract only, no planted edge cases) and one per reusable intermediate table; index in `Input\README.md` |
| `Input\testdata\expected\small\` | Generator ground truth. Interviewer-only answer-checking material, never pipeline input |
| `Output\` | Exercise outputs; use `Output\focused\<exercise>` |
| `Questions\` | Issued prompts (`Q1.txt`, `Q2.txt`, ...) |
| `User_Responses\` | Candidate notebooks (`Response_<n>.ipynb`) |
| `LLM_Metadata\` | Active guide, `LEARNING_PLAN.md`, focused `SESSION_NOTES.md`, topic menus and an inactive reference archive |

## Dataset

Regenerate the data from the workspace root:

```text
python -m pip install -e .\tools\datagen
python -m data_generator --profile small --out Input --clean
```

The generator (v2.0.0) is a copy of the SupportIQ generator with three changes:
- **Uniform encoding.** All text is plain ASCII English. There are no other languages or scripts, no emoji, no invisible Unicode controls and no invalid bytes. One drifted call-centre CSV keeps a UTF-8 BOM, because that is a header-handling case.
- **Complex types (S34)** for array, struct, map and nested-JSON practice.
- **Data-modelling sources (S35–S37)** for CDC, SCD 1–4, star/snowflake and Data Vault practice: a CRM customer-profile CDC feed, a product catalog, an agent roster, a billing account key map, and early-arriving facts in tickets and calls.

**Reference run.** This is the interviewer's verification run: Python 3.13.9, seed `20260915`, anchor `2026-09-15`, self-check OK, determinism byte-identical.

```text
B001 written  files=90  rows=42263  {'LOADED': 90}
B002 written  files=10  rows=7425   {'LOADED': 10}
B003 written  files=15  rows=8130   {'LOADED': 14, 'DUPLICATE': 1}
B004 written  files=20  rows=19684  {'REJECTED': 6, 'LOADED': 14}
B005 written  files=7   rows=6390   {'LOADED': 7}
B006 written  files=2   rows=479    {'DUPLICATE': 2}
```

- **Totals:** 144 data files and 84,371 rows (about 68 MB).
  - Helpdesk: 101 files, 39,903 rows, `arrival_date=2026-09-02` … `2026-09-17`.
  - Call centre: 15 files, 9,926 rows, `2026-09-03` … `2026-09-15`.
  - CRM snapshots: 5 files, 23,042 rows, `2026-09-12` … `2026-09-16`.
  - CRM CDC: 16 files, 1,680 lines (1 initial snapshot file, 14 daily files, 1 replay copy), `2026-09-02` … `2026-09-17`.
  - Product catalog: 3 snapshots, 122 rows. Agent roster: 2 full-history extracts, 865 rows. Billing key map: 2 extracts, 8,833 rows.
- **Batches:**
  - B001: 12-day backfill.
  - B002: normal day.
  - B003: drift, ordering and duplicates.
  - B004: incident volume and six rejected files.
  - B005: corrected re-deliveries.
  - B006: replay of an identical helpdesk file and an identical CDC file.
- **Reproducibility:** the same seed, configuration and Python minor version give byte-identical files. If the candidate's run log differs from the reference run (for example a different Python minor version), recompute any answer key from their `Input`.
- **Scale profiles:** `medium` (90 days, about 2M helpdesk rows) and `large` (180 days, about 10M) remain available for scale and skew labs.

### Source schemas (state the needed subset explicitly in every prompt)

**Helpdesk JSONL.** One ticket snapshot per line, with a `.ctl.json` control file next to each data file (`file_name`, `record_count`, `sha256`, `generated_at`, `schema_version`).

| Field | Type |
|---|---|
| `ticket_id`, `created_at`, `updated_at`, `resolved_at`, `status`, `priority`, `channel`, `product`, `subject`, `description`, `assignee_id`, `category`, `schema_version` | STRING |
| `reopen_count`, `csat` | INT |
| `requester` | STRUCT<customer_id STRING, email STRING, locale STRING> |
| `tags` | ARRAY<STRING> |
| `comments` | ARRAY<STRUCT<comment_id STRING, author STRUCT<type STRING, id STRING>, created_at STRING, public BOOLEAN, body STRING, attachments ARRAY<STRUCT<file_name STRING, content_type STRING, size_bytes BIGINT>>>> |
| `sla` | STRUCT<policy STRING, targets ARRAY<STRUCT<metric STRING, target_minutes INT, breached BOOLEAN>>, pause_windows ARRAY<ARRAY<STRING>>> |
| `custom_fields` | JSON object with a variable key set; read it as MAP<STRING,STRING> |

Helpdesk timestamp contract: ISO-8601 with `Z` or a numeric offset and optional milliseconds; or `yyyy-MM-dd HH:mm:ss` meaning UTC; or epoch milliseconds as a digit string. Comments accumulate across a ticket's versions, and the export keeps the 20 most recent.

**Call-centre CSV** (header, RFC 4180). Columns: `CALL_ID, CUST_NO, CALL_START, CALL_END, AGENT, QUEUE, PRIORITY_CD, DISPOSITION, HANDLE_SECS, NOTES, PRODUCT_LINE, CSAT, IVR_JSON`. Times are `dd/MM/yyyy HH:mm` in Asia/Kolkata. `IVR_JSON` is JSON text inside a CSV column with this structure: STRUCT<entry STRING, menu_path ARRAY<STRING>, wait_secs ARRAY<INT>, callback_offered BOOLEAN, transfers ARRAY<STRUCT<queue STRING, secs INT>>>.

**CRM CSV.** A full daily snapshot with columns `customer_id, customer_name, tier, region, industry, signup_date, is_active`.

**CRM customer-profile CDC JSONL** (`crm/customer_changes/arrival_date=.../crm_customer_changes_<ts>_<seq>.jsonl`). One change event per line:

| Field | Type |
|---|---|
| `event_id`, `op` (`r` initial snapshot, `c` create, `u` update, `d` hard delete), `commit_ts` (system time, ISO-8601 UTC with ms), `customer_id`, `effective_from` (business date `yyyy-MM-dd`) | STRING |
| `source_seq` (commit order) | BIGINT |
| `changed_fields` | ARRAY<STRING> (empty for a no-op update) |
| `before`, `after` | STRUCT<segment STRING, account_manager STRING, city STRING, credit_limit INT, status STRING> (null when not applicable) |

The first file holds the initial snapshot (op `r`); then one file per business day, delivered the next day at 03:00 UTC.

**Product catalog CSV** (`reference/products/...`, full snapshots): `product_code, product_name, family_code, family_name, business_unit, launch_date, retired_date, is_active`. The hierarchy is product → family → business unit.

**Agent roster CSV** (`reference/agents/...`, full-history extracts): `agent_id, agent_name, team, site, role, effective_from, effective_to`. Dates are inclusive, and an empty `effective_to` means the row is open as of the extract.

**Billing account map CSV** (`billing/account_map/...`, full extracts): `billing_account_id, customer_id, account_type, currency, payment_terms, valid_from, valid_to`. Dates are inclusive, and an empty `valid_to` means open.

### Production handling of `_corrupt_record` (interviewer teaching reference)

- **Where malformed text goes.** In `PERMISSIVE` mode, Spark can put malformed input text into the configured corrupt-record string field. Keep that field in the explicit reader schema when it must be retained.
- **Keep curated tables clean.** Normal typed or curated tables should not hold raw corrupt payloads.
- **Quarantine.** Route malformed rows to a quarantine Delta table with the source file, record position, ingest run, rule or reason, and a short display-safe preview.
- **Keep the originals.** Landing files stay immutable for replay and forensics.
- **Inspecting in SQL viewers.** Project the ordinary business columns and a bounded sample, and avoid selecting the full raw payload column.

References: [Spark JSON corrupt-record behavior](https://spark.apache.org/docs/3.5.7/sql-data-sources-json.html), [Spark binary file source](https://spark.apache.org/docs/latest/sql-data-sources-binaryFile.html).

## Data model

```text
CRM customer snapshot (many versions by snapshot date)
   1 ──< helpdesk ticket snapshot (many versions per ticket_id)
   │        1 ──< comment (array element) 1 ──< attachment (array element)
   │        1 ──1 sla 1 ──< target (array element), 1 ──< pause window (array of [start, end])
   │        1 ──1 custom_fields (map, variable keys)
   └──< call-centre call (one logical row per call_id; corrections and re-exports exist)
            1 ──1 IVR journey (JSON text) 1 ──< menu step / wait, 1 ──< transfer

customer_id 1 ──< customer-profile change event (CDC; separate profile system, overlapping key set)
customer_id 1 ──< billing account (key map, valid_from/valid_to) ── second business key: billing_account_id
product_code (ticket.product, call.PRODUCT_LINE) >── 1 product ──> family ──> business unit (snowflake hierarchy)
agent_id (ticket.assignee_id, call.AGENT) >── 1 agent version valid at the event date ──> team, site
```

- **CRM customer.** `customer_id` is the key, and each CSV is a full snapshot. Customers can be added, removed, deactivated or change tier. A partial snapshot is rejected as a whole, and missing rows in a rejected snapshot are not deletions.
- **Helpdesk ticket.** `ticket_id` is the key, with many versions over time. The deterministic newest version is ordered by `updated_at`, then delivery, then a content hash. An identical later delivery is a no-op, and conflicting content with the same key and same timestamp exists on purpose.
- **Call.** `call_id` is the key. A later correction or re-export can arrive; pick a deterministic winner and keep lineage. `CUST_NO` can be null.
- **Customer to ticket or call** is one-to-many. Customer keys can be null, blank, a placeholder or unmatched, so unmatched facts must not be silently dropped by an inner join.
- **Nested cardinality.** Exploding comments, attachments, targets or pause windows multiplies rows. State the grain before any aggregation or join.
- **CDC profile.** `customer_id` is the key in a separate profile system. Not every CRM customer has a profile, and some profile ids never exist in CRM. Apply events in `source_seq` order after de-duplicating `event_id`.
- **Dimensions.** Product attributes change between catalog snapshots. Agent attributes are effective-dated rows, so a fact joins to the agent version valid at the fact's date.
- **Billing.** A customer has 0–3 billing accounts, and an account can move to another customer over time. This is the second business key for Data Vault hubs and links.
- **File lineage.** A file has many records. `_source_file`, `_record_no`, `_file_id`, delivery time and run id support reconciliation and replay.

## Deliberate data edge cases (interviewer-only)

Catalogue: `D:\Project_AI\PySpark_Hands_On\tools\datagen\data_generator\scenarios\catalog.py`. Per-record truth: `Input\testdata\expected\small\<batch>\record_labels.jsonl.gz`. CDC answer keys: `Input\testdata\expected\small\warehouse\cdc_customer_profile_final.csv` and `..._scd2.csv`. Not every scenario appears in every batch.

| Scenario | Deliberate condition | Practice implications |
|---|---|---|
| S01–S03 | Padded or mixed-case values; alternate timestamp encodings; missing optional fields (including `comments` and `custom_fields`) | Trim and case-normalize, multi-format parsing, absent vs invalid |
| S04–S11 | Null, empty or blank keys; bad or missing timestamps; future or ancient dates; invalid enums; unknown product; cast overflow or out-of-range values; resolution before creation | Required-key rules, `try_cast`, null vs invalid, quarantine vs nullify vs warn |
| S12 | Very long, empty, multi-line and HTML text (ASCII only) | Text limits, string cleaning, safe sampling, no unbounded collect |
| S13–S14 | CSV quoting and delimiter errors, truncated JSON or CSV, garbage lines | Corrupt-record strategy, explicit schema, parse vs DQ classification |
| S15, S18–S19 | Exact duplicates, same key and timestamp with different content, a byte-identical file under a new name | Idempotency, content hash, deterministic window ordering |
| S16–S17 | Ticket versions; a late older version delivered after a newer one | Latest-record CDC, stale-update protection |
| S20 | Later call-centre correction | Upsert ordering, replay-safe correction |
| S21–S23 | Files with about 2% and 12% malformed lines; control-count mismatch or a missing control file | File-level thresholds, control checks, no partial ingest of rejected files |
| S24–S28 | Extra, missing or renamed fields; type drift; CSV column reorder, header case and BOM; US date re-export | Schema evolution, name-based mapping, drift detection, date ambiguity |
| S29 | Hot customers and product, placeholder customer, incident day ×8, a mega ticket with very many versions and long nested arrays | Skew, salting, partition sizing, pathological keys |
| S30–S31 | Empty, prompt-injection, PII, log-dump and ambiguous text; fault-marker tokens | Regex extraction and masking, string parsing |
| S32 | Partial, duplicate or bad CRM snapshot rows | Snapshot completeness, duplicate keys, referential integrity |
| S33 | Identical templated descriptions | Text similarity vs duplicate records |
| S35 | CDC feed: events out of `source_seq` order inside a file; late events in the next day's file; duplicate events (same or next file); update delivered before its insert; delete then re-insert; two updates with the same `commit_ts`; no-op updates; back-dated `effective_from`; creates for ids not in CRM; one truncated line; a replayed identical file | Ordering by sequence not by file or time, idempotent MERGE, delete handling, SCD2 from CDC, system vs business time |
| S36 | Product catalog: a product re-parented to another family, a family renamed, a product retired, a late-added product already referenced by earlier tickets (`AI-COPILOT`), a dirty duplicate key (`" payments "`), products never catalogued (`EDGE-AGENT`, `VOICE-BOT`), and an unchanged catalog snapshot (no-op). Agent roster: transfers, one-day rows, overlapping and gapped intervals, two open rows, leavers, re-hires, an overlap corrected in the later extract, and agents missing from the roster (`AGT-0137`, `AGT-0242`, `AGT-0388`) | SCD1 vs SCD2 vs SCD3, snowflake vs star, unknown or inferred members, interval validation, point-in-time joins |
| S37 | Tickets and calls in the last days naming customers that CRM delivers only in a later snapshot (early-arriving facts); billing accounts with orphan, blank or mis-formatted customer keys, moved and shared accounts, duplicate rows | Late-arriving dimensions, inferred members, key normalization, Data Vault hubs/links/satellites, referential integrity |
| S34 | Nested and complex types. Empty, null and missing arrays (`comments` `[]` for API-created tickets, `null` and absent); three "no attachments" shapes (key missing, `[]`, `null`); duplicate and out-of-order comment elements; `size_bytes` as text (type drift inside a nested field); null `author` struct; tags with duplicate, padded, upper-case or null elements; `custom_fields` key-case drift (`Plan` vs `plan`), null map values and empty `{}`; `sla` absent for the social channel; open-ended pause windows (`[start, null]`); `IVR_JSON` empty or not valid JSON | `explode` vs `explode_outer` and row cardinality, `posexplode` vs time order, `array_distinct` and dedup after explode, `transform`/`filter`/`flatten`, `element_at`/`slice`/`size`, map functions, `from_json` and `schema_of_json`, nested schema enforcement and drift |

Other facts:
- Tickets can be reopened, and unresolved tickets exist.
- Helpdesk timestamps carry several offsets.
- Call-centre times are local IST, and one re-export uses `MM/dd/yyyy`.
- Customer and product frequencies are skewed (Zipf-like).

## Practice-first teaching

The goal is hands-on PySpark fluency and the judgment of a data architect. Use
[LEARNING_PLAN.md](LEARNING_PLAN.md), [Primary_Topics.txt](Primary_Topics.txt) and
[Spark_UI_Topics.txt](Spark_UI_Topics.txt) for the active learning path.
The curriculum is a menu of practical goals, not an exam checklist. No scores, pass targets, scheduled quizzes,
accuracy tracking or fixed repetition counts. Archived assessment instructions are inactive.

### Interaction contract

- State a brief agenda and give one small sub-problem whose answer normally needs 1–2 meaningful lines/statements
  of PySpark. Split longer work; do not code-golf. Supply setup/imports/schemas separately when needed.
- Specify the required result and available variables, not the solution recipe. Let the learner implement it.
- Review each snippet briefly; ask a focused counter-question when a material issue remains. Iterate freely,
  without a formal single-submission rule or a long design essay.
- "Skip", "do this one", "too easy" or "not engaging" authorizes the LLM to supply this step, explain its effect
  briefly, and keep the pipeline runnable. There is no penalty or mandatory follow-up question.
- Do not supply future steps' solutions or hints unless requested. A request to delegate the current step is not
  authorization to solve the entire exercise. Confirm execution before relying on supplied code's results.
- Occasionally ask one short architect-level decision question about grain, temporal meaning, quality, replays,
  consumer guarantees or cost. Keep it tied to the implementation; no quizzes appended to every reply.
- For a changed/unfamiliar case, ask for a brief prediction and test it through code/evidence. Exact API spelling,
  configuration defaults and exhaustive function catalogues are lookup material.

### UI checkpoints are part of the task

Before moving on from a relevant action, cache, shuffle, write or setting change, explicitly notify the learner
that there is a UI observation to make. Plan the checkpoint before the action when needed, inspect after it,
and preserve evidence before restarting, unpersisting or overwriting. This applies even when the LLM completed
an easy step. Skipping code does not waive a UI checkpoint; wait for the observation or an explicit request to skip UI.

Give one navigation move or one observation at a time: name the tab, identify the action by description/time/path,
and ask for one meaningful metric. Do not assume fixed execution IDs, job counts, tasks, file counts or timing.
File discovery/schema inference may create preparatory jobs before a DataFrame action; lazy row execution is
not a promise that the Jobs tab stays empty. Follow actual plans and link operators to the scheduler stage.
If nothing informative changed, say no UI inspection is needed rather than forcing a full tour.

### Exercise construction and validation

A prompt has a clear business contract, input paths/file selection, required schema, output grain/path/schema,
and measurable correctness checks. Then present only the next small coding task or UI observation during coaching.
Keep hidden dataset cases and reference answers private; do not give expected counts or a full solution in advance.
The task roadmap may be saved in the prompt for orientation, but do not send it as a batch of questions.

Default to Windows local Spark/Delta; check runtime support for platform-specific features. Supply explicit setup
and stable variable names. Preserve the learner's existing notebook work, input data and Delta versions.
A fresh kernel does not mean deleting a table. Record actual Delta version numbers and count snapshot-active files.
The cloud checkout is separate from the learner's Windows process; code supplied, executed and verified are distinct.

Validate the business result with an independent key or meaningful invariants before calling it correct. Prefer
bounded inputs and duplicate-aware comparisons. A performance improvement must preserve the result. Do not disable
checks or treat a delegated/unrun step as validated. No need for an artificial failure in every exercise.

For a focused comparison, keep input/business logic and cache conditions consistent, change one execution setting,
inspect evidence, and state the trade-off briefly. Runtime SQL settings differ from process startup settings:
changing driver memory reliably requires a new process/kernel, not just spark.stop() and a new builder.
Optional memory/failure labs need bounded data, an isolated capped process and a clear restore path.

At a natural stopping point, update SESSION_NOTES.md with only the current task, actual design decisions, unresolved
questions and pending UI checkpoint. Keep old records in the archive; do not resume historical quizzes or maintain
scores. For reusable validated output, update Input/intermediate/<output>.md and the Input index with actual schema,
grain, rows, producing exercise and Delta version. Preserve original candidate cells if adding review notes.

## Current exercise

Start fresh at [Questions/Q3.txt](../Questions/Q3.txt): customer-profile history from CDC, built through small
connected coding tasks. Follow SESSION_NOTES.md for the current step. There is no full-solution submission barrier.
