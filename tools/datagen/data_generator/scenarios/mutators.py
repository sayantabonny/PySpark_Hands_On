"""Record-level damage (S02-S11, S14). Each mutator changes exactly one thing and reports the
rule it is expected to trigger, so expectations never have to be reverse-engineered.

Mutators work on the rendered record (a dict for JSON, a column->string dict for CSV) and
receive the true values (datetimes) so they can re-render timestamps consistently.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from ..core import timefmt as tf
from ..labels.outcomes import ACCEPTED, ACCEPTED_NULLIFIED, QUARANTINE_DQ, QUARANTINE_PARSE


@dataclass
class Mutation:
    record_class: str
    scenarios: list[str]
    rules: list[str]
    expected: str
    detail: str | None = None
    overrides: dict = field(default_factory=dict)     # canonical values changed by the mutation


VALID = Mutation("VALID", ["S01"], [], ACCEPTED)
# renderings that standardise to exactly the same canonical row (same _record_hash as the clean one)
CANONICAL_EQUIVALENT = frozenset({None, "PAD", "LOWER", "PAD_ENUMS", "CASE_ENUMS", "TS_MILLIS", "TS_SPACE",
                                  "TS_EPOCH", "PAD_ID"})


# ============================================================================ helpdesk (JSON)
class HelpdeskMutator:
    def __init__(self, anchor: date):
        self._future = tf.at(anchor + timedelta(days=400), 10)

    def mild(self, rng: random.Random, rec: dict, created: datetime, updated: datetime,
             resolved: datetime | None) -> Mutation:
        kind = rng.choice(("PAD_ENUMS", "CASE_ENUMS", "TS_MILLIS", "TS_SPACE", "TS_EPOCH",
                           "DROP_OPTIONAL", "PAD_ID"))
        if kind == "PAD_ENUMS":
            for k in ("priority", "status", "channel"):
                rec[k] = f"  {rec[k]} "
        elif kind == "CASE_ENUMS":
            rec["priority"] = rec["priority"].upper()
            rec["status"] = rec["status"].capitalize()
            rec["channel"] = rec["channel"].upper()
        elif kind.startswith("TS_"):
            fmt = {"TS_MILLIS": tf.iso_millis, "TS_SPACE": tf.space_utc, "TS_EPOCH": tf.epoch_ms}[kind]
            rec["created_at"], rec["updated_at"] = fmt(created), fmt(updated)
            rec["resolved_at"] = fmt(resolved) if resolved else None
        elif kind == "DROP_OPTIONAL":
            for k in ("assignee_id", "tags", "csat", "comments", "custom_fields"):
                rec.pop(k, None)
            return Mutation("MILD", ["S03"], [], ACCEPTED, kind)
        else:
            rec["ticket_id"] = f" {rec['ticket_id']}  "
        return Mutation("MILD", ["S02"], [], ACCEPTED, kind)

    def nullify(self, rng: random.Random, rec: dict, created: datetime) -> Mutation:
        kind = rng.choice(("RESOLVED_BEFORE_CREATED", "CSAT_OUT_OF_RANGE", "REOPEN_HUGE"))
        if kind == "RESOLVED_BEFORE_CREATED":
            rec["resolved_at"] = tf.iso(created - timedelta(hours=3))
            return Mutation("NULLIFY", ["S08"], ["R-TKT-008"], ACCEPTED_NULLIFIED, kind)
        if kind == "CSAT_OUT_OF_RANGE":
            rec["csat"] = rng.choice((0, 6, 99))
            return Mutation("NULLIFY", ["S11"], ["R-TKT-009"], ACCEPTED_NULLIFIED, kind)
        rec["reopen_count"] = 5000
        return Mutation("NULLIFY", ["S11"], ["R-TKT-011"], ACCEPTED_NULLIFIED, kind)

    def dq_error(self, rng: random.Random, rec: dict) -> Mutation:
        kind = rng.choice(("KEY_NULL", "KEY_EMPTY", "KEY_BLANK", "CREATED_BAD", "UPDATED_MISSING",
                           "BAD_PRIORITY", "BAD_STATUS", "BAD_CHANNEL", "FUTURE", "ANCIENT"))
        rule, scenario = {
            "KEY_NULL": ("R-TKT-001", "S04"), "KEY_EMPTY": ("R-TKT-001", "S04"),
            "KEY_BLANK": ("R-TKT-001", "S04"), "CREATED_BAD": ("R-TKT-002", "S05"),
            "UPDATED_MISSING": ("R-TKT-003", "S05"), "BAD_PRIORITY": ("R-TKT-004", "S09"),
            "BAD_STATUS": ("R-TKT-005", "S09"), "BAD_CHANNEL": ("R-TKT-006", "S09"),
            "FUTURE": ("R-TKT-007", "S06"), "ANCIENT": ("R-TKT-007", "S07")}[kind]
        if kind.startswith("KEY_"):
            rec["ticket_id"] = {"KEY_NULL": None, "KEY_EMPTY": "", "KEY_BLANK": "   "}[kind]
        elif kind == "CREATED_BAD":
            rec["created_at"] = rng.choice(("2026-13-45T10:00:00Z", "yesterday", ""))
        elif kind == "UPDATED_MISSING":
            rec.pop("updated_at")
        elif kind == "BAD_PRIORITY":
            rec["priority"] = "URGENTISSIMO"
        elif kind == "BAD_STATUS":
            rec["status"] = "escalated-2"
        elif kind == "BAD_CHANNEL":
            rec["channel"] = "fax"
        elif kind == "FUTURE":
            future = self._future + timedelta(minutes=rng.randrange(60 * 24 * 30))
            rec["created_at"] = rec["updated_at"] = tf.iso(future)
            rec["resolved_at"] = None
        else:
            rec["created_at"] = rng.choice(("1970-01-01T00:00:00Z", "1999-03-01T08:00:00Z"))
        return Mutation("DQ_ERROR", [scenario], [rule], QUARANTINE_DQ, kind)


def corrupt_json_line(rng: random.Random, line: bytes) -> tuple[bytes, str]:
    """Parse-level damage (S14): the line can no longer be read as a JSON object."""
    if rng.random() < 0.7:
        cut = max(10, int(len(line) * rng.uniform(0.4, 0.8)))
        return line[:cut].rstrip(b"\n") + b"\n", "TRUNCATED_JSON"
    garbage = rng.choice((b"#### EXPORT ABORTED: worker lost connection ####",
                          b"PK..binary-chunk..%%%%@@@@ [unreadable attachment fragment]",
                          b"ticket_id=HD-0000;status=open;priority=high"))
    return garbage + b"\n", "GARBAGE_LINE"


# ============================================================================ call centre (CSV)
class CallcenterMutator:
    def __init__(self, anchor: date):
        self._future = tf.at(anchor + timedelta(days=400), 5)

    def mild(self, rng: random.Random, row: dict[str, str], start: datetime) -> Mutation:
        kind = rng.choice(("PAD", "LOWER", "END_EMPTY", "CSAT_EMPTY"))
        if kind == "PAD":
            row["PRIORITY_CD"] = f" {row['PRIORITY_CD']} "
            row["CUST_NO"] = f" {row['CUST_NO']} " if row["CUST_NO"] else row["CUST_NO"]
        elif kind == "LOWER":
            row["PRIORITY_CD"] = row["PRIORITY_CD"].lower()
            row["DISPOSITION"] = row["DISPOSITION"].lower()
        elif kind == "END_EMPTY":
            row["CALL_END"] = ""
            return Mutation("MILD", ["S03"], [], ACCEPTED, kind,
                            {"updated_at": start, "resolved_at": None})
        else:
            row["CSAT"] = ""
            return Mutation("MILD", ["S03"], [], ACCEPTED, kind)
        return Mutation("MILD", ["S02"], [], ACCEPTED, kind)

    def nullify(self, rng: random.Random, row: dict[str, str], start: datetime) -> Mutation:
        kinds = ["CSAT_OUT_OF_RANGE", "HANDLE_NEGATIVE", "HANDLE_HUGE", "HANDLE_OVERFLOW"]
        if row["DISPOSITION"] == "RESOLVED":
            kinds.append("END_BEFORE_START")
        kind = rng.choice(kinds)
        if kind == "CSAT_OUT_OF_RANGE":
            row["CSAT"] = rng.choice(("0", "6", "99"))
            return Mutation("NULLIFY", ["S11"], ["R-TKT-009"], ACCEPTED_NULLIFIED, kind)
        if kind == "HANDLE_NEGATIVE":
            row["HANDLE_SECS"] = "-30"
            return Mutation("NULLIFY", ["S11"], ["R-TKT-010"], ACCEPTED_NULLIFIED, kind)
        if kind == "HANDLE_HUGE":
            row["HANDLE_SECS"] = "1000000000000"
            return Mutation("NULLIFY", ["S11"], ["R-TKT-010"], ACCEPTED_NULLIFIED, kind)
        if kind == "HANDLE_OVERFLOW":                       # does not fit BIGINT -> try_cast -> NULL
            row["HANDLE_SECS"] = "99999999999999999999"
            return Mutation("NULLIFY", ["S11"], ["SYS-CAST"], ACCEPTED_NULLIFIED, kind)
        end = start - timedelta(minutes=20)
        row["CALL_END"] = tf.ist_dmy(end)
        return Mutation("NULLIFY", ["S08"], ["R-TKT-008"], ACCEPTED_NULLIFIED, kind, {"updated_at": end})

    def dq_error(self, rng: random.Random, row: dict[str, str]) -> Mutation:
        kind = rng.choice(("ID_BLANK", "START_BAD", "PRIORITY_BAD", "DISPOSITION_BAD", "FUTURE", "ANCIENT"))
        if kind == "ID_BLANK":
            row["CALL_ID"] = rng.choice(("", "   "))
            return Mutation("DQ_ERROR", ["S04"], ["R-TKT-001"], QUARANTINE_DQ, kind)
        if kind == "START_BAD":
            row["CALL_START"] = rng.choice(("N/A", "00/00/0000 00:00", "31-08-2026 10:00"))
            return Mutation("DQ_ERROR", ["S05"], ["R-TKT-002"], QUARANTINE_DQ, kind)
        if kind == "PRIORITY_BAD":
            row["PRIORITY_CD"] = rng.choice(("P9", "HIGH"))
            return Mutation("DQ_ERROR", ["S09"], ["R-TKT-004"], QUARANTINE_DQ, kind)
        if kind == "DISPOSITION_BAD":
            row["DISPOSITION"] = "TRANSFERRED"
            return Mutation("DQ_ERROR", ["S09"], ["R-TKT-005"], QUARANTINE_DQ, kind)
        if kind == "FUTURE":
            future = self._future + timedelta(minutes=rng.randrange(60 * 24 * 30))
            row["CALL_START"], row["CALL_END"] = tf.ist_dmy(future), tf.ist_dmy(future + timedelta(minutes=7))
            return Mutation("DQ_ERROR", ["S06"], ["R-TKT-007"], QUARANTINE_DQ, kind)
        row["CALL_START"], row["CALL_END"] = "01/01/1999 10:00", "01/01/1999 10:10"
        return Mutation("DQ_ERROR", ["S07"], ["R-TKT-007"], QUARANTINE_DQ, kind)

    @staticmethod
    def malformed(rng: random.Random, row: dict[str, str], columns: tuple[str, ...]) -> tuple[str, Mutation]:
        """Returns a raw CSV line. Spark's CSV reader does not flag token-count mismatches itself:
        an unquoted delimiter is caught by the reader's `_overflow` sentinel column (PARSE), a
        truncated row is caught by the NOT_NULL rules on priority and status (DQ)."""
        if rng.random() < 0.5:
            notes = (row["NOTES"] or "no notes").replace('"', "").replace("\n", " ").replace("\r", " ")
            values = [row[c] if c != "NOTES" else f"{notes}, see ticket, callback requested" for c in columns]
            line = ",".join(v if c == "NOTES" else _csv_field(v) for c, v in zip(columns, values))
            return line, Mutation("MALFORMED", ["S13"], ["SYS-PARSE"], QUARANTINE_PARSE, "UNQUOTED_DELIMITER")
        kept = [_csv_field(row[c]) for c in columns[:5]]
        return ",".join(kept), Mutation("MALFORMED", ["S14"], ["R-TKT-017", "R-TKT-018"],
                                        QUARANTINE_DQ, "TRUNCATED_ROW")


def _csv_field(value: str) -> str:
    if any(ch in value for ch in ',"\r\n'):
        return '"' + value.replace('"', '""') + '"'
    return value
