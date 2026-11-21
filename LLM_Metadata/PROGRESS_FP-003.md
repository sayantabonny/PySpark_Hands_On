# FP-003 progress log (source of truth for the current exercise)

Prompt: [../Questions/Q3.txt](../Questions/Q3.txt) · Plan: [LEARNING_PLAN.md](LEARNING_PLAN.md) · Resume: [SESSION_NOTES.md](SESSION_NOTES.md)

Rules for this file (see LEARNING_PLAN.md, "Tracking and anti-hallucination guardrails"):
- Record only what happened in the conversation. Learner code is copied verbatim.
- Numbers and UI observations are recorded only as **reported by the learner**, never as expected or assumed values.
- Every result carries a status:
  - **reported**: the learner gave the value or observation.
  - **supplied**: the instructor provided the code.
  - **unverified**: assumed, not confirmed.
  - **open**: asked, not yet answered.
- The instructor updates this file before sending each reply, and reads it (with SESSION_NOTES.md and Q3.txt) before every reply.

## Current state

| Item | Value | Status |
|---|---|---|
| Current step | S03 done; S03 job interpretation pending one UI confirmation | — |
| Next step | S04: count malformed lines into a variable (name to be agreed) | not issued |
| Open questions to learner | Job 2 detail: does it show the skipped 4-task stage plus a completed 1-task stage? | open |
| Kernel variables believed to exist | `spark`, `F`, `Window`, type imports, `profile_type`, `cdc_schema`, `CDC_PATH`, `OUT_PATH`, `cdc_df`, `raw` (cached), `input_lines` | from the learner's cells; `raw = cdc_df` not shown explicitly, so **unverified** |
| Runtime settings changed by us | none (AQE and shuffle partitions untouched until S23) | — |
| Spark app | 4.0.1, UI `http://host.docker.internal:4040` | reported |

## Recorded counts

| Variable | Meaning | Value | Status |
|---|---|---|---|
| `input_lines` | rows in `raw` (all delivered lines incl. malformed) | 1680 | reported; verified (matches the independent key) |

## Step log

### Setup (2026-11-21)
- Supplied by the instructor: a fresh-kernel session (`local[4]`, Delta 4.0.1, driver 4g) and `cdc_schema` (with `_corrupt_record`), `CDC_PATH`, `OUT_PATH`. Added to `Response_3.ipynb` under "FP-003 fresh start".
- Learner reported: `4.0.1 http://host.docker.internal:4040`; the UI opens. Status: reported.

### S01: read input with full source path
- Learner attempt 1:
  ```python
  cdc_df = spark.read.format("json").schema(cdc_schema).option("columnNameOfCorruptColumn", "_corrupt_record").load(CDC_PATH).withColumn("_source_file",F.col("_metadata.file_path"))
  ```
- Feedback: the option name was misspelled, and the result must be named `raw`. `_metadata.file_path` and reading from the folder root were correct.
- Learner attempt 2 (accepted):
  ```python
  cdc_df = spark.read.format("json").schema(cdc_schema).option("columnNameOfCorruptRecord", "_corrupt_record").load(CDC_PATH).withColumn("_source_file",F.col("_metadata.file_path"))
  ```
- UI checkpoint (Jobs, after the read): learner reported **no jobs**. Explanation given: with an explicit schema there is no inference scan, and listing 16 files happens on the driver below the parallel-listing threshold. Status: reported.
- Learner reasoning: "the misspelled option still worked because of lazy evaluation; it could fail on an action." Corrected: unknown options are ignored silently, and it worked because the default `spark.sql.columnNameOfCorruptRecord` is `_corrupt_record`. A different field name would have stayed null with no error.
- Not yet verified: the learner was asked to add `raw = cdc_df`; it is implied by S02 but was not shown.
- Not yet observed: reading the root folder may add a discovered `arrival_date` partition column; the learner has not printed the schema.

### S02: request caching
- Learner code: `raw = raw.cache()`
- UI checkpoint (Storage): learner reported **nothing listed**. Learner reasoning: cache is lazy until an action. Correct. Added: the cache is keyed by the query plan, not the variable name.

### S03: materialize and count input lines
- Learner code: `input_lines = raw.count()`
- UI checkpoint (Storage, Jobs): learner reported **3 jobs**, **Cached Partitions 4**, **Fraction Cached 100%**. Status: reported.
- Explanation given: the 16 small files were packed into 4 input partitions (split sizing uses about total bytes divided by default parallelism, capped by maxPartitionBytes), so the file count is not the partition count. 100% means every partition was materialized by the count.
- Learner reported `input_lines` = 1680 (verified against the independent key).
- Learner reported the jobs:
  - Job 0: `count at NativeMethodAccessorImpl.java:0`, 1 stage, 4 tasks
  - Job 1: `$anonfun$withThreadLocalCaptured$2 at CompletableFuture.java:1768`, 1 stage, 4 tasks
  - Job 2: same description, 1 stage, 4 tasks, marked skipped
- Instructor interpretation, given as a **hypothesis** and **unverified**:
  - Job 0 builds the cache: AQE materializes the cached relation first (a table-cache query stage), 4 tasks for 4 partitions.
  - Job 1 is the AQE shuffle-map stage (partial count per cached partition, then shuffle write), submitted from an AQE background thread, hence the CompletableFuture call site.
  - Job 2 is the final result job. Its map stage is shown as skipped because Job 1 already produced the shuffle output, and a 1-task final-aggregation stage is expected.
  - Confirmation requested: Job 2 detail.

## Design decisions agreed
- (none yet beyond the Q3 contract)

## UI evidence summary (learner-reported only)
| After step | Tab | Observation |
|---|---|---|
| S01 | Jobs | no jobs |
| S02 | Storage | empty |
| S03 | Jobs / Storage | 3 jobs (0: count at NativeMethodAccessorImpl; 1 and 2: CompletableFuture call site; 2 has a skipped stage); 4 cached partitions; 100% cached |
