"""Outcome vocabulary and the expected-result rules shared by all feeds.

Expectations are computed *by construction*: every mutation reports the rule it violates, and a
small set of base facts (placeholder customer, unknown product, blank or long text, drift) adds the
WARN-level hits every row carries. The rule ids are the DQ_RULE seeds of PDF 1, Section 17.7,
plus the two row-level NOT_NULL rules recommended in PDF 2 (R-TKT-017/018).
"""
from __future__ import annotations

import re
import zlib
from dataclasses import dataclass

from ..model.reference import PLACEHOLDER_CUSTOMER, PRODUCTS
from ..scenarios.catalog import FAULTS

# --- expected record outcomes -------------------------------------------------------------
ACCEPTED = "ACCEPTED"                        # parsed, passed DQ; silver keeps the newest accepted version
ACCEPTED_NULLIFIED = "ACCEPTED_NULLIFIED"    # accepted, one field set to NULL by a NULLIFY rule
ACCEPTED_SILENTLY_WRONG = "ACCEPTED_SILENTLY_WRONG"   # S28: loads with a wrong date, no rule can see it
DUPLICATE_NOOP = "DUPLICATE_NOOP"            # identical to an already accepted version
STALE_IGNORED = "STALE_IGNORED"              # older than the version already in silver (MERGE guard)
CONFLICT_DUPLICATE = "CONFLICT_DUPLICATE"    # S18: same key and timestamps, different content
QUARANTINE_PARSE = "QUARANTINE_PARSE"
QUARANTINE_DQ = "QUARANTINE_DQ"
FILE_REJECTED = "FILE_REJECTED"
FILE_DUPLICATE = "FILE_DUPLICATE"
ACCEPTING = (ACCEPTED, ACCEPTED_NULLIFIED, ACCEPTED_SILENTLY_WRONG)

# --- file statuses (FILE_MANIFEST) ----------------------------------------------------------
LOADED, REJECTED, DUPLICATE = "LOADED", "REJECTED", "DUPLICATE"

# --- LLM expectations with FakeLLM ------------------------------------------------------------
LLM_SUCCESS, LLM_SKIPPED = "SUCCESS", "SKIPPED"
_MARKER = re.compile(r"\[\[FAULT:[A-Z_=0-9]+\]\]")
_MARKER_TO_KIND = {f.marker: kind for kind, f in FAULTS.items()}


def llm_expected(subject: str | None, description: str | None) -> str:
    """Final enrichment status with FakeLLM (after all retries), from the text the pipeline sees."""
    text = f"{subject or ''} {description or ''}"
    if not text.strip():
        return LLM_SKIPPED
    match = _MARKER.search(description or "")
    if match and match.group(0) in _MARKER_TO_KIND:
        return FAULTS[_MARKER_TO_KIND[match.group(0)]].expected
    return LLM_SUCCESS


# --- WARN-level facts every evaluated row can carry -----------------------------------------
_CUSTOMER_RE = re.compile(r"^C\d{7}$")
_PRODUCTS = frozenset(PRODUCTS)


def warn_hits(customer_id: str | None, product: str | None, description: str | None,
              max_description: int) -> list[str]:
    hits = []
    if description is None or not description.strip():
        hits.append("R-TKT-012")                          # NOT_BLANK description
    elif len(description) > max_description:
        hits.append("R-TKT-013")                          # MAX_LENGTH description
    cid = (customer_id or "").strip() or None
    if cid is not None and not _CUSTOMER_RE.match(cid):
        hits.append("R-TKT-014")                          # REGEX customer_id
    if cid == PLACEHOLDER_CUSTOMER:
        hits.append("R-TKT-015")                          # REFERENCE: placeholder is not a CRM customer
    prod = (product or "").strip() or None
    if prod is not None and prod not in _PRODUCTS:
        hits.append("R-TKT-016")                          # ALLOWED_VALUES product_code (S10)
    return hits


@dataclass
class RecordLabel:
    """Ground truth for one emitted record (one line of a JSONL file or one CSV data row)."""
    source: str
    source_id: str | None
    version_no: int
    record_class: str
    scenarios: list[str]
    rules: list[str]                     # rule hits, incl. SYS-* built-in checks
    expected: str
    llm_expected: str | None = None
    detail: str | None = None
    # internal bookkeeping (not written): silver key and the MERGE ordering of this version
    key: int | None = None
    upd: int = 0                         # canonical updated_at, epoch seconds
    dlv: int = 0                         # _delivery_ts, epoch seconds
    token: int = 0                       # identity of the business content (stands in for _record_hash)

    def to_json(self, batch: str, file: str, record_no: int) -> dict:
        return {"batch": batch, "file": file, "record_no": record_no, "source": self.source,
                "source_id": self.source_id, "version_no": self.version_no, "class": self.record_class,
                "scenarios": self.scenarios, "rules": self.rules, "expected": self.expected,
                "llm_expected": self.llm_expected, "detail": self.detail}


def content_token(*parts: object) -> int:
    """Two renderings with the same token carry the same canonical business content."""
    return zlib.crc32("|".join(map(str, parts)).encode("utf-8"))
