# PySpark / Databricks Hands-on Practice Guide

## Purpose and candidate profile

Use this guide to coach a candidate with 8–10 years of experience through hands-on PySpark exercises on deterministic synthetic data.

- **Primary Objective:** Build confidence for a hands-on PySpark Databricks interview.
- **Secondary Objective (revised 2026-10-11 at the user's request):** Become proficient in hands-on coding for every topic in `Primary_Topics.txt`, and learn to read the Spark UI the way production engineers do.

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
| `LLM_Metadata\` | This guide, `SESSION_NOTES.md`, `Primary_Topics.txt`, `Spark_UI_Topics.txt`, `TOPIC_COVERAGE.csv`, `SPARK_UI_COVERAGE.csv`, `KNOWLEDGE_CHECKS.csv` (check log), `archive\` (superseded lists and trackers) |

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

## Curriculum and scheduling

`Primary_Topics.txt` is the curriculum authority. Its `Importance` tags drive selection. It was streamlined on 2026-11-21 from 322 function-level entries to **132 skills** (17 core objectives `C01`–`C17`, 115 operation skills `O<section>.<item>`), because a candidate is not expected to memorize every function in the AI-coding era. The functions in brackets give the scope only. The old list, old trackers and `TOPIC_ID_MAPPING.csv` (old ID → new IDs) are in `LLM_Metadata\archive\2026-11-21_before_streamlining\`. `Spark_UI_Topics.txt` is chained to every exercise through Parts B–D of the prompt (see "Interaction protocol"). Never skip a topic because no project uses it.

### Tiers and pass targets

| Tier | Sections (current tags) | Target demonstrated passes |
|---|---|---|
| Very High | 4 Window Functions; 20 Data Modeling, CDC & DW Patterns | 5 |
| High | 2 Joins; 6 Conditional Logic; 11 Pivot/Unpivot; 12 Partition-Aware; 19 Ad-hoc/Advanced; 21 Delta Optimization; 22 Delta Transactions; 23 Reconciliation & DQ; **all Core Objectives C01–C17** (treated as High) | 4 |
| Medium | 3, 5, 7, 8, 10, 13, 14, 15, 17 | 3 |
| Low | 1 DataFrame Basics; 9 String Operations; 16 Spark SQL & Expressions; 18 Sorting & Top-N | 3 |

`TOPIC_COVERAGE.csv` holds one row per skill: `Importance`, `Target_Passes`, `Old_IDs` (traceability) and `Pass_1`…`Pass_5_Evidence`. Migrated passes are grouped by exercise: one pass per exercise in which any of the skill's old sub-topics passed. When the user edits an importance tag in `Primary_Topics.txt`, re-sync both columns.

### Composing each exercise

Cover several topics in one exercise whenever the business requirement makes it natural. Every exercise must contain:

1. **Primary targets.** One or two topics from the Very High or High tier, chosen from the topics furthest from their target, with Very High first. Ties go to the topic practiced least recently.
2. **Supporting topics.** Medium topics that the requirement naturally needs. Prefer uncovered ones.
3. **Secondary objectives.** At least two Low-tier topics woven into the requirement in every exercise (for example a rename, a `selectExpr`, a string cleanup, a Top-N or a temp-view query). Prefer Low topics with the fewest passes. Do not label them as "secondary" in the prompt; make them part of the requirement.

### Cadence

- **Very High:** a Very High section appears as a primary or supporting target in at least every second exercise.
- **High:** after its first pass, every High section and Core Objective is revisited at least once in every eight exercises.
- **After reaching the target:** Very High and High topics keep coming back as supporting topics (spaced practice). Medium and Low topics then appear only when natural.
- **Variety:** vary the business context, data source (helpdesk JSON, nested arrays, call-centre CSV with JSON text, CRM snapshots), scale (small vs medium profile) or technical constraint on every revisit, so each pass shows transfer, not repetition.
- **Format:** keep each exercise to one bounded sitting, with no one-operation limit. Use compact, business-led requirements only.

### Module map (topic clusters and their natural Spark UI chain)

| Cluster | Primary topics | Natural Spark UI chain |
|---|---|---|
| Read, profile, normalize | explicit schemas; CSV/JSON/Parquet/Delta; inferSchema; schema validation and evolution; null/NaN; dirty data; EDA, cardinality, categorical profiling; metadata columns and surrogate keys | lazy evaluation, schema-inference cost, predicate and column pruning, input partitions |
| Columns and complex types | select/selectExpr/withColumn(s)/rename/drop/alias/filter/where; conditionals; strings and dates; arrays, structs, maps, nested JSON; `transform`, `create_map`, `to_json`, `explode` and cardinality | narrow transformations, Catalyst, codegen, built-in vs UDF |
| Quality, keys, dedup, CDC | distinct/dropDuplicates; window winner; latest/earliest; SCD 1–4; CDC, upsert, late data, soft deletes; reconciliation, checksums, audit | shuffle from dedup, window and merge; Exchange; skew and spill; Delta log |
| Joins and relationships | all join types; multi-key, expression and null joins; fan-out; aggregation + join; referential integrity; gap and anti-joins | broadcast vs sort-merge vs hash; thresholds and hints; AQE; dynamic partition pruning; salting |
| Windows and time analytics | frames; lag/lead/first/last; running and rolling; sessionization, gaps, sequences; YoY/MoM; Top-N | window exchange and sort, stage boundaries, task skew, shuffle partitions |
| Aggregation and reshape | conditional and multiple aggregations; collect_list/set; pivot and unpivot; statistics and correlation | wide transformations, exchange, spill, whole-stage codegen |
| Partitioning and scale | repartition/coalesce/range/partitionBy; file layout; join optimization; skew and salting | input vs shuffle vs output partitions, AQE, executor sizing, memory |
| Execution APIs and memory | accumulators and broadcast variables; foreachPartition/mapPartitions/map/flatMap/toLocalIterator/collect; driver vs executor; UDF vs Pandas UDF | job boundaries, serialization, driver OOM lab, GC, spill |
| Data modelling and CDC | SCD 1/2/3/4, CDC processing, upserts, soft deletes, late-arriving data, surrogate keys, fact/dimension, star vs snowflake, Data Vault, incremental and watermark loads, idempotency | MERGE scan and rewrite metrics, shuffle from window-based ordering, Delta transaction log, file counts |
| Delta delivery | create/read/write, append/overwrite/merge/update/delete, idempotency, schema enforcement and evolution, OPTIMIZE/ZORDER/stats/data skipping/partition pruning, VACUUM, time travel, clone | file counts and scan metrics before and after, transaction log |

## Interaction protocol (revised 2026-10-21)

Each exercise takes three turns:

| Turn | Who | Content |
|---|---|---|
| **Interaction 1** | Interviewer | One prompt (`Questions\Q<n>.txt`) with four parts. **A:** the question plus a brief function overview. **B:** the Spark config change that triggers a performance or error scenario. **C:** the exact UI locations to observe and report. **D:** an explanation of the bottleneck or error and how to fix it. |
| **Interaction 2** | Candidate | The code (notebook with saved outputs and counts) and the Part C report: metrics from the "before" run (scenario triggered) and the "after" run (fix applied). |
| **Interaction 3** | Interviewer | A single-round review: run-validated, annotated in the notebook, design questions answered, Part C metrics explained (see "Evaluation, correction and tracking"). |

### Part A: the question and the function overview

Every prompt states:
- the exact environment
- the input path and file selection
- the complete schema and data types needed
- the business rules
- measurable acceptance criteria
- the output artifact, path and schema
- the expected submission

When an output is both a table and a path, give the SQL-safe table identifier and the filesystem path separately.

Default to Windows local Spark. Use Databricks only for genuinely Databricks-only features (Unity Catalog, Auto Loader, Delta Live Tables, Workflows, platform governance).

Keep prompts free of planted edge cases, scenario IDs, hints and solution recipes. State ordinary business rules and required outcomes, and do not name the function that solves a requirement unless the exercise specifically tests that API. Save each prompt as `Questions\Q<n>.txt`.

Assess interview-relevant skills such as `_source_file`, `_record_no`, `_file_id`, recursive file discovery, explicit schemas and corrupt-record handling. Never require parsing arbitrary filename tokens or remembering arbitrary constants. Supply derived metadata as an input instead.

Ask for code only: a notebook with saved outputs in `User_Responses\Response_<n>.ipynb`, plus the required counts. Do not ask the candidate for a written design explanation (see "Design questions" below). Never give a full solution before the attempt.

**Carry-forward rule.** The design questions answered in the review of exercise *n* become **coding requirements** in exercise *n+1*. For example, "how is the winner deterministic?" becomes "prove determinism by rerunning and comparing the outputs". Fold them into the next prompt's business requirements and acceptance criteria. Never present them as quiz questions.

**Function overview (part of every Part A).** List the functions and options the candidate *may or may not* need, with a one-line description each: what it does, its key argument, and its main gotcha (for example ANSI-mode `cast` errors in Spark 4, null handling, session-time-zone rendering, determinism).
- Where there are several reasonable ways to reach a goal, list them briefly, with one trade-off each, and let the candidate choose. Do not say which is best before the attempt.
- The overview must not reveal the planted edge cases or the full solution. It describes tools, not the recipe.
- When a review's corrected code uses non-obvious constructs (for example regular expressions or `expr` SQL), explain them token by token in the notebook.

### Part B: the Spark config that triggers a scenario

`Spark_UI_Topics.txt` is about performance monitoring and failure diagnosis, so the candidate must first **experience** the bottleneck or error, then fix it. Every prompt therefore gives explicit config instructions for a "run 1" that triggers the scenario, chosen to match the Part C focus. Examples:
- disable AQE: `spark.sql.adaptive.enabled=false`
- keep or raise `spark.sql.shuffle.partitions`
- disable broadcast: `spark.sql.autoBroadcastJoinThreshold=-1`, or force broadcast with a hint
- fewer cores: `local[1]` / `local[2]`
- lower `spark.driver.memory` / `spark.executor.memory`, or `spark.memory.fraction`
- caching on or off
- a skewed key, or `repartition(n)` extremes

For each change, state whether it is a **runtime** setting (`spark.conf.set`, no restart) or a **startup** setting: master, driver or executor memory, which need `spark.stop()` and a new session.

"Run 2" applies a fix from Part D, and the candidate reports both runs.

Safety: failure and OOM scenarios use a bounded workload, an isolated or fresh session, heap caps such as `512m`–`1g` (never larger inputs without bound), and outputs under `Output\`. State how to restore the normal session afterwards.

### Part C: what to observe and report

Name the exact UI locations:
- the tab
- how to find the query or stage (by its action or description)
- the node, metric or percentile to read

Ask for a prediction first. Ask for the same two to six metrics for run 1 and run 2, plus any non-UI evidence such as Delta file counts or the error message and stack-trace line. Never ask for a full walk-through.

Choose the focus from `Spark_UI_Topics.txt`: the least-covered section that the exercise genuinely exercises, following the progression and depth levels in "Spark UI coverage" below.

### Part D: what happens and how to fix it

Explain the bottleneck or error the candidate is about to see:
- the physical cause (shuffle partitions, task overhead, spill, OOM, broadcast size, skew, small files)
- the expected symptoms in the UI
- interviewer-measured numbers from a scratchpad run of the same scenario, when available
- two to four fixes, with trade-offs

The candidate picks one for run 2. Verify the scenario by running it in the scratchpad before issuing the prompt, so the symptoms described are real.

### Evaluation, correction and tracking (single round; protocol revised 2026-10-16)

- **One submission per exercise.** If the code is wrong or incomplete, do **not** return it for rework. The review closes the exercise.
- **Validate by running.** Check the submission against an independent answer key computed from `Input`, which covers the hidden edge cases. Wherever possible, read the candidate's actual outputs with local Spark (`C:\Spark\spark-4.0.1-bin-hadoop3`, Delta jars in the Ivy cache) and run the corrected code in the scratchpad before presenting it.
- **Annotate the candidate's notebook in place.** Leave the candidate's cells untouched.
  - **Mistake notes.** After each faulty cell, insert a markdown cell tagged `interviewer-review`, headed `❌ M<k> (cell above): <title>`. State what is wrong, the measured impact (counts and an example key) and the correct approach.
  - **Review summary.** Add one markdown cell with a table of expected vs candidate vs corrected results, and a list of what was done correctly.
  - **Corrected solution.** Add a `✅ Corrected solution (validated)` section of code cells tagged `interviewer-fix`. They must run in a fresh kernel after the session cell, and must not depend on earlier kernel state. Follow them with the validated run output.
  - **Design questions.** Add a markdown cell where the interviewer **asks and answers** the design questions for the exercise: why this approach, alternatives, determinism, time zones, performance and Spark internals. It ends with the carry-forward list for the next exercise.
- **Correct the outputs.** When the corrected code changes an output that later exercises may reuse, write the corrected table to the same `Output\focused\` path as a new Delta version, so the candidate's versions stay available through time travel. Then update `Input\intermediate\<output>.md`.
- **Passes** (no candidate explanation is required any more):
  - A topic passes when the candidate's **own** code implements it correctly, and the part of the acceptance criteria that depends on it is met.
  - Topics that were correct only after the interviewer's fix do not pass. List them in `SESSION_NOTES.md` as "corrected by interviewer" so they come back in later exercises.
  - Low-tier topics pass on correct use.
  - Never a pass: a mention, an unattempted prompt, or the interviewer's own code or explanation.
- **Record keeping.** Record each pass in the next empty `Pass_n_Evidence` column of `TOPIC_COVERAGE.csv`, starting with the exercise ID. Then update `SESSION_NOTES.md`.
- **Intermediate tables.** When an evaluated exercise writes an output that later exercises can reuse, add `Input\intermediate\<output>.md` and a row in `Input\README.md`. Include the location, format, table identifier, producing exercise, grain, row count, Delta versions, columns with types, and the rules that produced derived columns, all taken from the candidate's actual output. If a source schema changes, update its `Input\*.md`. A prompt may point to these files, but must still state the schema it needs.

- **Part C review.** In the notebook review, explain the candidate's run 1 vs run 2 metrics against Part D. Confirm or correct their readings, and record UI depth in `SPARK_UI_COVERAGE.csv`. Reporting the metrics correctly is L1. Choosing a fix and getting the predicted improvement is L2. L3 needs the candidate to diagnose an unannounced or varied scenario in a later exercise, or tune a second knob.

### Spark UI coverage (used by Parts B–D)

Do not ask for a complete UI walk-through. The aim is to go deeper step by step and cover all of `Spark_UI_Topics.txt` over the course of the exercises. The list was streamlined on 2026-11-21 into **12 core concepts `U01`–`U12`** and **12 labs `L01`–`L12`**. Each lab names its trigger (Part B), what to observe (Part C) and the concepts it exercises. `L12` (production reading) applies to every lab.

1. **Pick the focus.** Choose one lab (occasionally two closely linked ones) that the exercise's own code genuinely exercises. Prefer the lab whose concepts have the lowest `Depth_Reached` in `SPARK_UI_COVERAGE.csv`. Follow a sensible progression:
   - L01 / L03: jobs, stages, tasks
   - L04 / L05: partitions, shuffle, Exchange
   - L02: scans and pruning
   - L06 / L07: joins, SQL tab, AQE
   - L08: caching
   - L09: skew
   - L10: executors, memory, spill
   - L11: failure lab
2. **Ask for a prediction first.** Before opening the UI, the candidate answers one question, such as "how many jobs will this action create?" or "will the window add a shuffle?".
3. **Give an exact location.** State the tab, how to find the right row or query (by its description or action, not a fixed ID), and the element to read: a plan node, stage metric, task summary percentile, Storage entry or Environment property.
4. **Ask two to four precise questions** about values or observations at that location. Examples: "number of tasks in the stage that contains the Window operator", "Shuffle Write size of that stage", "partition count after AQEShuffleRead", "max vs median task duration".
5. **Teach.** Explain the physical behavior behind the reported evidence, connect it to the code and the plan, and correct misconceptions. Do not claim fixed job, stage or task counts without the candidate's evidence; they vary with Spark version, AQE, local parallelism, caching and actions.
6. **Raise depth over time.** Revisit a section at the next depth level in a later exercise:
   - **L1 Locate & read:** find the right place and read the metric.
   - **L2 Explain:** connect the metric to the code or plan and say why.
   - **L3 Diagnose & tune:** change one setting or code path, predict the effect, rerun and compare. Example: `spark.sql.shuffle.partitions`, a broadcast threshold or hint, caching, `repartition`.

   A section counts as covered at L2. Aim for L3 on core objectives such as shuffle, AQE, skew, joins, partitions, memory and spill.
7. **Record it.** In `SPARK_UI_COVERAGE.csv`, record the depth demonstrated, the exercise ID and the evidence. A section the interviewer taught without a candidate report goes in `Taught_In` only.

Windows safety for failure and memory labs: run them in an isolated Spark process with a JVM heap cap, a bounded workload and outputs under `Output\`. Driver memory is a startup setting, so change it in a new process. Never stress the whole OS or the hardware.

The candidate's Windows Spark setup: `local[6]` (the notebook used `local[4]`), executor memory `8g`, driver memory `4g`, Python worker fault handlers enabled, `spark.sql.debug.maxToStringFields=2000`, Spark 4.0.1 with Delta 4.0.1.

## Knowledge checks: attention, recall and reasoning (added 2026-11-21)

**Purpose.** While teaching, periodically test the candidate's **attention** (catching key details), **memory** (recalling earlier concepts) and **reasoning** (solving unfamiliar problems). Track accuracy, weaknesses and improvement without interrupting the learning flow.

### Question types

| Type | Tests | Source | Good example | Not allowed |
|---|---|---|---|---|
| **ATTENTION** | Noticing a detail that matters for correctness | The prompt, review or metrics from the last one or two turns | "Q3 orders a customer's events by one field. Which, and what goes wrong if you use commit_ts instead?" | Trivia: row counts, file names, exact numbers that do not change a decision |
| **RECALL** | Retrieving an earlier concept without notes | A concept practised or taught at least one exercise earlier, chosen by spaced repetition | "Why did max(row_number()) over an ordered window return 1?" | Function signatures, default values or option names to recite |
| **REASONING** | Applying concepts to an unfamiliar scenario: predict, diagnose, choose, design | New situations built from mastered or in-progress concepts, often outside the current dataset | "A job's stage has 200 tasks of 5 ms and writes 200 files from 2 MB of data. What happened, and what is the cheapest fix?" | Re-asking a solved exercise in the same words |

Mix: roughly a third of each over time. Prefer REASONING once a concept has two correct RECALLs.

### Rules

- **One question at a time**, at most one per interviewer turn. It goes at the **end** of the turn, after all exercise content, in a block headed `🧠 Quick check K<nnn> (<TYPE>)`, so it never splits instructions.
- **When.** Ask in every teaching turn: Interaction 1 (prompt), Interaction 3 (review), and explanation or meta turns. Skip only when the turn is a pure file or admin operation.
- **Answer format.** Answerable in one to three sentences or one line of code, without running Spark.
- **No rote memorization.** Ask about mechanisms, consequences, trade-offs and diagnoses. Use a concrete detail only when it changes a decision.
- **No repeats.** Never ask the same question twice. A concept is re-tested with a new scenario or angle.
- **No hints unless the candidate asks** ("hint"). Then give one minimal hint and set `Hint_Used = Y`; the maximum score becomes 0.5.
- **No leaks.** A check must not reveal the planted edge cases or the solution of an exercise still in progress. Ask about past exercises, general concepts, or parts of the current prompt that are already explicit.
- **Pre-registered key points.** Write `Key_Points` (two or three gradable statements) in `KNOWLEDGE_CHECKS.csv` *before* asking. The candidate should not open that file while a check is pending.
- **Answering is optional and asynchronous.** The candidate can answer at the start of their next message, alongside any work, or say `skip`. A skip is logged and not scored. An unanswered check is carried once into the next turn, then logged as skipped.

### Grading and feedback (light touch)

- **Score** against the key points: **1** (all), **0.5** (partial or hint used), **0** (wrong or missing).
- **Reply** at the top of the next turn, in at most three lines: the verdict, the missing or incorrect key point, and one line on the correct idea. No lecture; continue the exercise flow straight after.
- **Log** the answer summary, the score and the misconception (if any) in `KNOWLEDGE_CHECKS.csv`.

### Spaced repetition and adaptivity

- **Leitner boxes per concept.** Next due after **1** exercise in box 1, **2** in box 2, **4** in box 3, **8** in box 4.
  - Correct (1): move the concept up one box.
  - Partial (0.5): it stays in its box.
  - Wrong (0): it goes back to box 1.

  New concepts start in box 1 when first taught or practised. `Box` and `Next_Due_After` (an exercise number) are stored per check row; the concept's current box is in its latest row.
- **Choosing the next check:**
  1. RECALL of a due concept, weakest first.
  2. Otherwise ATTENTION on the latest turn.
  3. Otherwise REASONING on an in-progress concept.

  Rotate types so that no type is asked three times in a row.
- **Difficulty** runs from 1 (one idea) to 3 (combining two or three concepts, or an unfamiliar scenario). After two correct answers in a row in a type, raise it; after two misses, lower it.
- **Feedback into exercises.** A concept with an average below 0.67 over at least two checks is a **weakness**. It becomes a supporting topic in the next exercise, and its RECALL is rescheduled to box 1.

### Learner profile (in `SESSION_NOTES.md`)

Update after each graded check:
- accuracy overall and by type (all-time and the last 10)
- the current weaknesses with their misconceptions
- the strongest concepts
- the trend: improving, flat or declining, comparing the last 10 checks with the previous 10

Report a one-line summary in each Interaction 3 review, and in more detail every 10 checks.

## One-line resume prompt

"Resume the PySpark hands-on practice using `D:\Project_AI\PySpark_Hands_On\LLM_Metadata\SESSION_NOTES.md`: give one concise, fully specified exercise at a time on Windows local Spark/Delta. Build each from Very High/High primary targets plus supporting Medium topics and at least two Low topics. Keep planted cases hidden and evaluate against an answer key. Each prompt has four parts: A, the question with a brief function overview (alternatives listed; I choose); B, the Spark config that triggers a performance or error scenario; C, the exact UI metrics to report for the before and after runs; D, the bottleneck and its fixes. I reply with code and the Part C report. Validate in one round by running it against an answer key, annotate mistakes and validated fixes in my notebook, answer the design questions yourself and carry them into the next exercise as coding requirements, and track topic and Spark UI coverage toward their targets. End each teaching turn with one knowledge check (attention, recall or reasoning; spaced repetition; no hints unless I ask), grade my answer in at most three lines at the start of your next turn, and keep KNOWLEDGE_CHECKS.csv and the learner profile up to date."
