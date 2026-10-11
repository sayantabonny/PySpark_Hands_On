"""Scenario catalogue (PDF 1, Section 22.7) and the LLM fault contract (S31).

Scenario ids are stable: they appear in record labels, file expectations and test names.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Scenario:
    sid: str
    label: str      # valid | DQ-invalid | malformed | duplicate | schema-drift | skewed | LLM-problematic | nested
    title: str


SCENARIOS: dict[str, Scenario] = {s.sid: s for s in (
    Scenario("S01", "valid", "Baseline records"),
    Scenario("S02", "valid", "Padding, mixed-case enums, alternative timestamp formats"),
    Scenario("S03", "valid", "Missing optional fields"),
    Scenario("S04", "DQ-invalid", "Business key NULL, empty or whitespace-only"),
    Scenario("S05", "DQ-invalid", "Unparseable created_at / missing updated_at"),
    Scenario("S06", "DQ-invalid", "created_at in the future"),
    Scenario("S07", "DQ-invalid", "Very old created_at"),
    Scenario("S08", "DQ-invalid", "resolved_at before created_at (NULLIFY)"),
    Scenario("S09", "DQ-invalid", "Invalid priority / status / channel"),
    Scenario("S10", "DQ-invalid", "Unexpected but plausible product code (WARN)"),
    Scenario("S11", "DQ-invalid", "Numeric out of range, overflow, not castable (NULLIFY / SYS-CAST)"),
    Scenario("S12", "valid", "Text extremes: long, empty, newlines, HTML (all ASCII English)"),
    Scenario("S13", "malformed", "CSV quoting and delimiter problems"),
    Scenario("S14", "malformed", "Truncated JSON, garbage lines, truncated CSV rows"),
    Scenario("S15", "duplicate", "Exact duplicate record (same file / later file)"),
    Scenario("S16", "valid", "Ticket versions progressing across exports and batches"),
    Scenario("S17", "duplicate", "Late older version arriving after a newer one"),
    Scenario("S18", "duplicate", "Same key and timestamps, different content"),
    Scenario("S19", "duplicate", "Byte-identical file under a new name"),
    Scenario("S20", "valid", "Call-centre correction delivered later"),
    Scenario("S21", "malformed", "File with ~2% malformed lines (below threshold)"),
    Scenario("S22", "malformed", "File with ~12% malformed lines (above threshold)"),
    Scenario("S23", "malformed", "Control file count mismatch / control file missing"),
    Scenario("S24", "schema-drift", "Extra field / extra column"),
    Scenario("S25", "schema-drift", "Required field missing in the whole file"),
    Scenario("S26", "schema-drift", "Required field renamed (created_at -> createdAt)"),
    Scenario("S26B", "schema-drift", "Optional field renamed (description -> desc)"),
    Scenario("S27", "schema-drift", "Type drift, CSV column order, header case, BOM"),
    Scenario("S28", "schema-drift", "Call-centre re-export in MM/dd/yyyy"),
    Scenario("S29", "skewed", "Placeholder / hot customers, hot product, incident day, mega ticket"),
    Scenario("S30", "LLM-problematic", "Empty, injection, PII, logs, ambiguous"),
    Scenario("S31", "LLM-problematic", "FakeLLM fault markers"),
    Scenario("S32", "DQ-invalid", "CRM snapshot problems: partial, duplicates, bad values"),
    Scenario("S33", "valid", "Templated identical descriptions"),
    Scenario("S34", "nested", "Complex types: empty/null/missing arrays, duplicate and unordered elements, nested type drift, map key drift, null structs, JSON text inside CSV"),
    Scenario("S35", "cdc", "CDC feed: out-of-order, late and duplicate events, update before insert, delete and "
             "re-insert, same-timestamp updates, no-op updates, back-dated effective dates, orphan keys"),
    Scenario("S36", "dimension", "Dimension history: product re-parenting, family rename, retirement, late-added "
             "member, dirty duplicate key; agent roster overlaps, gaps, two open rows, leavers, re-hires, missing agents"),
    Scenario("S37", "keys", "Keys and identity: early-arriving facts, billing key map with orphans, blank and "
             "mis-formatted keys, moved and shared accounts, duplicate rows"),
)}

LLM_PROBLEM_KINDS: tuple[tuple[str, float], ...] = (
    ("EMPTY_TEXT", 15.0), ("PROMPT_INJECTION", 15.0), ("PII_HEAVY", 20.0),
    ("CODE_LOG_DUMP", 15.0), ("AMBIGUOUS", 10.0), ("FAULT_MARKER", 15.0))
CALL_LLM_PROBLEM_KINDS: tuple[tuple[str, float], ...] = (
    ("EMPTY_TEXT", 30.0), ("PII_HEAVY", 35.0), ("CODE_LOG_DUMP", 15.0), ("FAULT_MARKER", 20.0))


@dataclass(frozen=True)
class Fault:
    marker: str
    expected: str   # outcome with FakeLLM once all retries have been exhausted or succeeded


# Contract for FakeLLMClient: 'UNTIL_ATTEMPT=n' fails while the record-level attempt number < n.
# Record-level retry policy assumed (PDF 1, 16.5): 3 attempts for transient errors,
# 2 for invalid output, 1 for content filtering.
FAULTS: dict[str, Fault] = {
    "RATE_LIMIT_2": Fault("[[FAULT:RATE_LIMIT_UNTIL_ATTEMPT=2]]", "SUCCESS_AFTER_RETRY"),
    "TIMEOUT_2": Fault("[[FAULT:TIMEOUT_UNTIL_ATTEMPT=2]]", "SUCCESS_AFTER_RETRY"),
    "SERVER_ERROR_3": Fault("[[FAULT:SERVER_ERROR_UNTIL_ATTEMPT=3]]", "SUCCESS_AFTER_RETRY"),
    "RATE_LIMIT_4": Fault("[[FAULT:RATE_LIMIT_UNTIL_ATTEMPT=4]]", "PERMANENT_FAILURE:RATE_LIMITED"),
    "INVALID_JSON": Fault("[[FAULT:INVALID_JSON]]", "PERMANENT_FAILURE:INVALID_JSON"),
    "ENUM_VIOLATION": Fault("[[FAULT:ENUM_VIOLATION]]", "PERMANENT_FAILURE:SCHEMA_VIOLATION"),
    "CONTENT_FILTER": Fault("[[FAULT:CONTENT_FILTER]]", "PERMANENT_FAILURE:CONTENT_FILTERED"),
    "TRUNCATED": Fault("[[FAULT:TRUNCATED]]", "PERMANENT_FAILURE:OUTPUT_TRUNCATED"),
}
