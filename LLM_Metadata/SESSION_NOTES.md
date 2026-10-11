# PySpark Hands-on Session Notes

## Persistent coaching instructions

Follow [`HANDS_ON_INTERVIEW_GUIDE.md`](HANDS_ON_INTERVIEW_GUIDE.md). Candidate profile: 8–10 YOE.

**Objectives (secondary objective revised 2026-10-11 at the user's request):**

- Primary Objective: Build confidence for a hands-on PySpark Databricks interview.
- Secondary Objective: Become proficient in hands-on coding for every topic in `Primary_Topics.txt`, and read the Spark UI like a production engineer. This practice is standalone and separate from the SupportIQ and IncidentIQ projects. Do not frame exercises as project modules or prepare project claims.

**Exercise design:**

- Give one concise, fully specified exercise at a time.
- Cover several topics in one exercise when the requirement makes it natural. Each exercise has:
  - one or two **Very High/High** primary targets (Core Objectives count as High)
  - **Medium** supporting topics where natural
  - at least **two Low-tier topics** woven in as secondary objectives
- Cadence and pass targets are in the guide:
  - Very High = 5 passes, High and Core = 4, Medium = 3, Low = 3.
  - A Very High section appears in at least every second exercise.
  - Every High section is revisited within every eight exercises after its first pass.
  - Very High and High topics keep recurring after reaching their target.
- Each prompt gives:
  - the business requirement
  - the environment
  - the exact input location and file selection
  - the required schema and data types
  - the output artifact, path and schema (table identifier and filesystem path stated separately)
  - the acceptance criteria
  - the expected submission

  Save prompts as `Questions\Q<n>.txt`.
- Keep prompts free of planted edge cases, scenario IDs, hints and solution recipes. Never require parsing arbitrary filename tokens or remembering arbitrary constants. Do assess `_source_file`, `_record_no`, `_file_id`, recursive discovery, explicit schemas and corrupt-record handling when they are selected topics.

**Interaction:**

1. **Part 1.** The candidate implements and explains. The interviewer evaluates against an independent answer key.
2. **Part 2.** A targeted Spark UI drill, only after evaluation:
   - Ask for one specific UI location: the tab, how to find the row or query, and the element or metric.
   - Ask for a prediction first, then two to four precise report-back questions.
   - Teach from the reported evidence.
   - Never ask for a complete walk-through.
   - Choose the least-covered relevant section of `Spark_UI_Topics.txt`. Deepen it over time (L1 locate & read, L2 explain, L3 diagnose & tune) until every section is covered.

**Tracking:**

- `TOPIC_COVERAGE.csv`: one row per atomic topic, with `Importance`, `Target_Passes` and `Pass_1`…`Pass_5_Evidence`.
  - Primary and supporting topics pass with a correct implementation, an explanation and met acceptance criteria.
  - Low-tier secondary topics pass with correct use plus a one-sentence justification.
  - A mention or the interviewer's explanation alone is never a pass. Vary the context across passes.
- `SPARK_UI_COVERAGE.csv`: `Depth_Reached` 0–3 with evidence per level. Interviewer-only teaching goes in `Taught_In`.
- When the user edits importance tags in `Primary_Topics.txt`, re-sync `Importance` and `Target_Passes`. Last synced 2026-10-11: Joins and Conditional Logic are now High; Window Functions and Data Modeling/CDC are Very High.

**Environment:**

- Default to Windows local Spark/Delta. Use Databricks only for Databricks-only services.
- Failure and OOM labs use an isolated Spark process, a JVM heap cap, a bounded workload and outputs under `Output\`.

## Workspace, dataset and environment

Workspace: `D:\Project_AI\PySpark_Hands_On` (its own git repo, remote `sayantabonny/PySpark_Hands_On`; nothing committed yet).

- `tools\datagen\` — data generator v2.0.0 (distribution `handson-datagen`, module `data_generator`). It was copied from the SupportIQ generator on 2026-10-11 and modified:
  - all text is ASCII English (removed: Hindi, Japanese, Arabic, Spanish, German and Hinglish text; emoji; zero-width and RTL marks; invalid UTF-8 bytes; binary garbage lines; non-ASCII customer names)
  - scenario S34 complex types added: helpdesk `comments`/`sla`/`custom_fields`, dirty `tags`, call-centre `IVR_JSON`
  - data-modelling sources added the same day at the user's request (they asked whether Data Modeling/CDC was covered): S35 CRM customer-profile CDC feed (`crm/customer_changes`), S36 product catalog (`reference/products`) and agent roster (`reference/agents`), S37 billing key map (`billing/account_map`) and early-arriving customers in tickets and calls. CDC answer keys: `Input\testdata\expected\small\warehouse\cdc_customer_profile_final.csv` and `_scd2.csv`
  - the self-check and the tests verify that every landing file is ASCII
  - 16 unit tests pass (including an independent CDC replay against the answer key), the wheel builds, and `--verify-determinism` is byte-identical
- `Input\landing\` — practice data, regenerated by the user on 2026-10-11 (log matches the reference run; all 244 landing files are byte-identical to it) with:
  ```text
  python -m pip install -e .\tools\datagen
  python -m data_generator --profile small --out Input --clean
  ```
  The FP-002 answer key below applies as-is.
- `Input\testdata\expected\small\` — ground truth after regeneration (interviewer-only).
- `Output\focused\<exercise>` — outputs. FP-001's Delta table is at `Output\focused\FP-001_queue_action`; there is also the earlier `Output\round1`.
- `Questions\` — prompts (`Q1.txt`, `Q2.txt`). `User_Responses\` — notebooks (`Response_1.ipynb` = FP-001; it still uses the old SupportIQ paths as a historical record).

**Reference regeneration (interviewer verification run, Python 3.13.9, seed `20260915`, anchor `2026-09-15`).** 144 data files and 84,371 rows:
- helpdesk: 101 files, 39,903 rows, arrival 2026-09-02 … 09-17
- call centre: 15 files, 9,926 rows, 09-03 … 09-15
- CRM snapshots: 5 files, 23,042 rows, 09-12 … 09-16
- CRM CDC: 16 files, 1,680 lines (1,614 distinct events; final state 1,043 profiles; SCD2 answer key 1,558 rows)
- products: 3 snapshots, 122 rows; agents: 2 extracts, 865 rows; billing: 2 extracts, 8,833 rows

Batch log: B001 90/42,263; B002 10/7,425; B003 15/8,130 (1 DUPLICATE); B004 20/19,684 (6 REJECTED); B005 7/6,390; B006 2/479 (2 DUPLICATE).

If the user's regeneration log differs (for example a different Python minor version), recompute every answer key from their `Input`.

**Windows local Spark config supplied by the candidate:**

```python
spark = (SparkSession.builder
        .appName("Practical_Test")
        .master("local[6]")
        .config("spark.executor.memory", "8g")
        .config("spark.driver.memory", "4g")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .config("spark.sql.debug.maxToStringFields", 2000).getOrCreate())
```

**Runtime:**
- PySpark/Spark 4.0.1 and Delta 4.0.1 (`io.delta:delta-spark_2.13:4.0.1`); the notebook used `local[4]`.
- No Hive support, so the catalog is in memory: table names disappear on restart, but paths persist.
- Spark UI at `http://localhost:4040`. Teach from actual run evidence; never assert fixed job, stage or task counts.

## Current position — FP-002 issued, awaiting attempt (data regenerated 2026-10-11)

- **FP-001** closed 2026-10-07 with 26 first passes (CO-a; OP-01.01/.03/.06; OP-03.01/.02/.04; OP-06.01/.02; OP-09.02/.03/.18; OP-14.01/.02/.04/.07/.09/.10/.19; OP-15.01–.05/.09; OP-23.01).
  - Not passed: OP-06.08, OP-22.01, CO-l.
  - Spark UI for FP-001 was taught by the interviewer (groupBy Exchange, AQE coalescing, SQL plan, Storage Delta state), so it is recorded under `Taught_In` only and no UI depth is demonstrated yet.
- **FP-002** issued 2026-10-07: current helpdesk ticket state with reject classification.
  - Prompt: `D:\Project_AI\PySpark_Hands_On\Questions\Q2.txt`. The window is `arrival_date=2026-09-02` … `2026-09-08` and the prompt is unchanged by the regeneration.
  - Targets: CO-f/CO-d/CO-m, OP-04 latest record and dedup, OP-05 latest per key and row_number, CO-b `_source_file`, CO-c/OP-08 multi-format timestamps and time zone, CO-l dirty data, OP-03.14 aggregation + window, OP-23 reconciliation.
  - Low secondaries present: trim/string cleaning, select/withColumn.
- **Spark UI drill planned for FP-002 (after evaluation).** Focus: UI-OP-04 Understanding Stages plus UI-CO-f Exchange, at L1→L2.
  - Prediction: does the window add a shuffle, and where is the stage boundary?
  - Location: SQL / DataFrame tab → the query of the Delta write → the Window node and the Exchange beneath it.
  - Report back: the Exchange partitioning expression; the number of tasks in the stage that contains Window; that stage's Shuffle Read size; whether AQEShuffleRead coalesced the partitions.
  - Optional follow-up later: scan PartitionFilters / files read for the `arrival_date` selection (UI-CO-l / UI-OP-02).

### Interviewer-only answer key for FP-002 (from the reference regeneration; do not reveal before submission)

Recompute this after the user regenerates, if their log differs.

- **Input:** 41 `*.jsonl` + 41 `.ctl.json` files (the control files must be excluded); 13,004 non-blank lines.
- **Rejects by precedence:** MALFORMED_RECORD 67, MISSING_TICKET_ID 81 (null, empty or whitespace), INVALID_UPDATED_AT 37 (all null).
- **Valid:** 12,819 = sum(version_count).
- **Output:** 4,386 distinct trimmed ticket_ids; max version_count 42 (the mega ticket).
- **Hidden cases:**
  - updated_at encodings: Z 10,606; +hh:mm 1,450; -hh:mm 404; `yyyy-MM-dd HH:mm:ss` 132; `.SSSZ` 119; epoch-ms 108
  - 18 tickets whose last-delivered record is older than an earlier one (a stale late version)
  - 37 tickets where the same UTC instant appears in different files (the file-path tie-breaker decides)
  - 42 tickets with the same instant in the same file, all content-identical (the final tie-breaker only needs to be deterministic)
- **Probe:**
  - session time zone vs the stored instant
  - partition filter vs path list vs recursiveFileLookup
  - excluding the `.ctl.json` files
  - row_number vs rank or dropDuplicates nondeterminism
  - a missed timestamp format, which shifts rows into INVALID_UPDATED_AT
- **Superseded keys:** first v2 run (before S35–S37) 12,967 lines, rejects 69/84/37, 4,381 output rows; pre-regeneration window 13,018 lines with rejects 81/81/36 and 4,382 output rows; June window 114,552 lines.

### Other notes

- The user showed that DBeaver/DuckDB `delta_scan` works with explicit columns, while `SELECT *` fails on raw corrupt payloads. Use the guide's `_corrupt_record` notes if relevant.
- Coverage: 26 of 322 topics have pass 1; no Spark UI section has demonstrated depth yet.

## Round log

### FP-001 — classify helpdesk tickets for queue handling

Final evaluation (2026-10-07): notebook writes Delta to `...focused\FP-001_queue_action` (external table `FP_001_queue_action`; delta log `isManaged:false`; v0 + v1 CREATE OR REPLACE; 8 parquet files = 4 live + 4 logically removed). Readback by path: 4 STRING columns, 10,983 rows = input. Counts: STANDARD_QUEUE 7,702; ESCALATE 1,439; CLOSED_NO_FOLLOW_UP 1,223; WAITING_FOR_CUSTOMER 500; NEEDS_REVIEW 119. The interviewer's lenient-decode Python oracle said 7,704/117; the difference is two NEW/OPEN+NORMAL lines with invalid UTF-8 (160000Z line 2489, 200000Z line 1860) that Spark treats as malformed → nulls → NEEDS_REVIEW. Candidate result is correct. Explanation: precedence answer reached the right conclusion (rules 1–4 are mutually exclusive, so reordering has no effect; the interviewer's "give a pair that changes" question was flawed) but reasoned via "no OR"; fallback answer omitted null semantics and 1:1 reasoning; did not know identifier vs path. Taught: first-match CASE semantics, null propagation through isin/when, PERMISSIVE malformed rows, narrow 1:1 projection, managed vs external tables, in-memory catalog, hyphen in SQL identifiers, overwrite = logical remove + VACUUM.

Primary topic: Core Objective `CO-a`, Conditional Data Manipulation. The exercise combined conditional rules, string normalization, JSON reading, file discovery, Delta writing, and grouped validation; this is allowed because future exercises may mix operations according to topic importance.

Environment: Windows local Spark with winutils/Delta.

Input: `D:\Project_AI\PySpark_Hands_On\Input\landing\helpdesk\tickets\arrival_date=2026-06-18\*.jsonl` (folder not included in the moved Input subset; FP-001 originally ran against the same files under `D:\Project_AI\SupportIQ\.lake\landing`); read only `ticket_id STRING, status STRING, priority STRING` from each JSON record. Other source attributes are outside this exercise.

Output: overwrite Delta at `D:\Project_AI\PySpark_Hands_On\Output\focused\FP-001_queue_action`, schema `ticket_id STRING, status STRING, priority STRING, queue_action STRING`.

Rules: compare trimmed, case-insensitive status and priority values while preserving the original input strings. `SOLVED`/`CLOSED` → `CLOSED_NO_FOLLOW_UP`; `PENDING`/`ON-HOLD` → `WAITING_FOR_CUSTOMER`; `NEW`/`OPEN` with `URGENT`/`HIGH` → `ESCALATE`; `NEW`/`OPEN` with `NORMAL`/`LOW` → `STANDARD_QUEUE`; all other or missing combinations → `NEEDS_REVIEW`. Apply these rules in the listed precedence order.

Acceptance: one output row per input row; no rows removed or added; `queue_action` contains only the five specified values and follows the rule precedence. Candidate reports total input/output rows and counts by `queue_action`, and explains the chosen conditional logic and how the fallback behaves.

Candidate implementation evidence: notebook defines a full JSON schema including `_corrupt_record`, sets `recursiveFileLookup=True`, `pathGlobFilter="*.jsonl"`, and `columnNameOfCorruptRecord`; reads all ticket folders and selects ticket_id/status/priority. It normalizes comparison columns with trim/upper, creates queue_action, drops temporary normalized fields, overwrites a Delta table, and reports counts.

Evaluation of first attempt: conditions used OR where both fields had to match, and the input scope was the parent directory. Candidate reports those bugs fixed. In the saved notebook now, the input path is the specified `arrival_date=2026-06-18` directory and both classification conditions use AND. Initial count output is 10,983. The table identifier `FP_001_queue_action` is SQL-safe. Keep it distinct from the output directory: Q1 calls for `...focused\FP-001_queue_action`, while the latest Storage UI still shows `...focused/FP_001_queue_action`. The latest notebook file has no persisted write/count/grouped-count execution outputs, so the final row count and category counts are not available from the saved artifact yet.

The two NEW/OPEN classification conditions use OR between status membership and priority membership. The business policy requires both fields to match the corresponding pair. As written, e.g. NEW + LOW falls into ESCALATE, and an unrelated status + URGENT can also be escalated. The resulting counts therefore do not prove the policy. Trim/upper comparison logic and preserving original status/priority are sound. The schema and corrupt-record option are present; this attempt does not demonstrate quarantine or corrupt-payload retention in the output. Ask candidate to fix the rule logic and scope/path and explain the decisions before recording a pass. No passes are recorded.

First-run Spark UI evidence: Spark 4.0.1, `local[4]`, driver/executor memory 4g/8g. The old Jobs page showed 14 completed jobs, Job 0 listing 92 paths/92 tasks, and a JSON scan of 556 files / 2,010,629 rows / 1,410.1 MiB. This was the incorrect broad-scope run and is retained only as comparison.

Latest corrected-run Spark UI evidence: app started 2026-10-03 20:20:51, Spark 4.0.1, `local[4]`; Jobs page showed 9 completed jobs. Query 12 (`groupBy(queue_action).count().show()`) completed in 0.9 s with Jobs 11 and 12. Its plan showed Scan JSON (10,983 rows, 5 files, 8.1 MiB) → Project nodes → partial HashAggregate (20 rows) → Exchange (200 partitions, 1,436 B / 20 records) → AQEShuffleRead (coalesced to 1 partition, 1,487 B) → final HashAggregate (5 groups) → CollectLimit. Stage 20 had 4 scan/partial-aggregate tasks; Stage 22 had one final task. No spill was reported. The post-write table count was Query 9; its plan displayed a LocalTableScan returning one scalar row and no Spark jobs, so the result value must be taken from the notebook cell rather than inferred from that plan. The Storage tab showed Delta Table State #3 and Delta Table State with Stats #3 under `...focused/FP_001_queue_action/_delta_log`; these are Delta snapshot metadata, not the ticket rows themselves.

The candidate requested a voice-agent UI walkthrough. No voice or speech-output tool is available in this session; the walkthrough was delivered in chat, with the Spark UI tab left open on Query 12.

Topics completed: see FP-001 final evaluation above (26 first passes). Use `TOPIC_COVERAGE.csv` to track the 322 atomic entries currently extracted from the primary topic list; retain the list's priority/order. Select future exercises with the guide's importance-based composition and cadence rules. Continue Windows by default; reserve Databricks for service-dependent operations. Add bounded Windows error scenarios when they serve the primary-topic curriculum.
