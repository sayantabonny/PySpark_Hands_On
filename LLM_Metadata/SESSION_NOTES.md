# Current practice session

## Focus

FP-003: build a customer-profile history from a change feed.
Prompt: [Questions/Q3.txt](../Questions/Q3.txt).
Plan: [LEARNING_PLAN.md](LEARNING_PLAN.md).
Start point: fresh kernel and setup, then one input sub-problem at a time. No complete implementation is assumed.

## How we work

- One small coding task per turn, normally 1–2 meaningful lines/statements.
- Explain the required result; let the learner choose the implementation. Supply setup separately when needed.
- The learner may ask for a hint, explanation, "do this one" or "skip" at any time.
- For a delegated step, provide just that runnable step and carry its result forward after execution is confirmed.
- Pause for relevant Spark UI evidence before proceeding, including delegated work. Skipping code does not skip UI.
- Give one UI navigation instruction or observation at a time. An explicit "skip UI" can defer it.
- Occasionally ask one concise architect-level question tied to the code or a changed scenario.
- No scoring, quiz schedule, pass targets or repetition quotas. No unsolicited hints or full future solutions.

## Working context

- Runtime: Windows local Spark 4.0.1 + Delta 4.0.1. Use local[4] for the focused comparison.
- Workspace: `D:\Project_AI\PySpark_Hands_On`.
- Input: `Input/landing/crm/customer_changes/`, all arrival folders and JSONL data files.
- Contract: [Input/crm_customer_changes.md](../Input/crm_customer_changes.md).
- Notebook: `User_Responses/Response_3.ipynb`; preserve existing cells/work.
- Output: `Output/focused/FP-003_customer_profile_scd2`; table `FP_003_customer_profile_scd2`.
- Preserve source files and Delta history. Record actual versions for comparison; do not assume 0 and 1.
- The cloud checkout does not expose the learner's Windows kernel; execution and UI observations need evidence.

## Resume state

**Instructor: read this file, [PROGRESS_FP-003.md](PROGRESS_FP-003.md) and `Questions/Q3.txt` before every reply,
and update them before sending (guardrails in LEARNING_PLAN.md).**

- Done: setup (reported), S01 read with `_source_file` (reported), S02 cache request (reported), S03 materialize/count (reported).
- `input_lines` = 1680 (reported; matches the key). Waiting on the learner: Job 2's stage detail, to confirm the hypothesis about the 3 jobs after S03.
- Next: S04, count malformed lines (after the open items).
- Unverified: whether `raw = cdc_df` ran; whether an `arrival_date` column was discovered.
