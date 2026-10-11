"""Turns record labels into the ground truth the pipeline is tested against:

    <profile>/generation_manifest.json
    <profile>/<batch>/file_expectations.csv      one row per delivered file
    <profile>/<batch>/batch_expectations.json    state after all files of the batch are processed
    <profile>/<batch>/record_labels.jsonl.gz     one line per record (small/medium profiles)
    <profile>/llm_golden.jsonl                   evaluation set for prompt/model changes

Record outcomes are provisional until their file is closed: only then is it known whether the
file is loaded at all, and whether an accepted version is newer than what silver already holds.
Key state advances at batch end, because the pipeline deduplicates a whole batch before MERGE.
"""
from __future__ import annotations

import csv
import gzip
import io
import json
from collections import Counter
from dataclasses import dataclass, field

from ..config.settings import GeneratorConfig
from ..writers.layout import ExpectedLayout
from .outcomes import (ACCEPTING, CONFLICT_DUPLICATE, DUPLICATE, DUPLICATE_NOOP, FILE_DUPLICATE,
                       FILE_REJECTED, LLM_SUCCESS, LOADED, QUARANTINE_DQ, QUARANTINE_PARSE, REJECTED,
                       STALE_IGNORED, RecordLabel)

CALLCENTER_KEY_BASE = 10 ** 15
FILE_COLUMNS = ("file", "source", "expected_status", "reason", "rows", "parse_quarantine", "dq_quarantine",
                "nullified", "rule_hits", "scenarios", "control_file", "expected_after_fix")


def callcenter_key(call_id: str) -> int:
    return CALLCENTER_KEY_BASE + int(call_id[2:])


@dataclass
class FileLedger:
    """Labels of one file while it is being written. At close the file's counts are folded into
    the batch totals and the labels are streamed out, so memory holds one file, not one batch."""
    batch: str
    rel_path: str
    source: str
    scenarios: list[str]
    control: str = "none"                  # present | missing | mismatch | none
    keep_labels: bool = False              # the file is copied later (S19): keep its labels
    status: str | None = None
    reason: str | None = None
    after_fix: str | None = None
    rows: int = 0
    labels: list[RecordLabel] = field(default_factory=list)
    file_hits: Counter = field(default_factory=Counter)
    expectation_row: list | None = None

    def add(self, label: RecordLabel) -> None:
        self.labels.append(label)

    @property
    def parse_failures(self) -> int:
        return sum(1 for lab in self.labels if lab.expected == QUARANTINE_PARSE)


class KeyState:
    """Newest accepted version per silver key, emulating the MERGE of PDF 1 (9.8) with the
    content guard recommended in PDF 2: a later delivery of *identical* content is a no-op."""

    def __init__(self, enabled: bool):
        self.enabled = enabled
        self.state: dict[int, tuple[int, int, int]] = {}     # key -> (updated, delivery, content)
        self.llm: dict[int, str] = {}                         # only non-SUCCESS outcomes are stored
        self._best: dict[int, tuple[int, int, int, str | None]] = {}

    def resolve(self, lab: RecordLabel) -> None:
        if not self.enabled or lab.key is None:
            return
        if lab.expected not in ACCEPTING + (DUPLICATE_NOOP, CONFLICT_DUPLICATE):
            return
        prior = self.state.get(lab.key)
        if lab.expected in ACCEPTING and prior is not None:
            upd, dlv, token = prior
            if lab.upd < upd or (lab.upd == upd and lab.dlv < dlv and lab.token != token):
                lab.expected = STALE_IGNORED
                return
            if lab.upd == prior[0] and lab.token == prior[2]:
                lab.expected = DUPLICATE_NOOP
                return
        best = self._best.get(lab.key)
        if best is None or (lab.upd, lab.dlv) > best[:2]:
            self._best[lab.key] = (lab.upd, lab.dlv, lab.token, lab.llm_expected)

    def end_batch(self) -> tuple[int, int]:
        inserted = updated = 0
        for key, (upd, dlv, token, llm) in self._best.items():
            prior = self.state.get(key)
            if prior is None:
                inserted += 1
            elif (upd, dlv) > prior[:2] and not (upd == prior[0] and token == prior[2]):
                updated += 1
            else:
                continue
            self.state[key] = (upd, dlv, token)
            if llm and llm != LLM_SUCCESS:
                self.llm[key] = llm
            else:
                self.llm.pop(key, None)
        self._best.clear()
        return inserted, updated

    def newest_updated(self, key: int) -> int | None:
        prior = self.state.get(key)
        return prior[0] if prior else None

    def summary(self) -> dict:
        helpdesk = sum(1 for k in self.state if k < CALLCENTER_KEY_BASE)
        outcomes = Counter(self.llm.values())
        outcomes[LLM_SUCCESS] = len(self.state) - sum(outcomes.values())
        return {"keys_after": len(self.state), "helpdesk_keys": helpdesk,
                "callcenter_keys": len(self.state) - helpdesk,
                "enrichment_after_drain": dict(sorted(outcomes.items()))}


class ExpectationRecorder:
    def __init__(self, cfg: GeneratorConfig, layout: ExpectedLayout):
        self._cfg = cfg
        self._layout = layout
        self.keys = KeyState(cfg.profile.track_keys)
        self.customers: dict[str, bool] = {}     # silver.customer: id -> is_active
        self._labels_fh = None
        self._golden: list[dict] = []
        self._batches: list[dict] = []
        self._thresholds = dict(cfg.contract.batch_fail_pct)

    # ------------------------------------------------------------------ batch lifecycle
    def begin_batch(self, batch: str, write: bool) -> None:
        self._batch, self._write = batch, write
        self._files: list[FileLedger] = []
        self._rows: Counter = Counter()          # row totals of the batch
        self._hits: Counter = Counter()          # rule hits in loaded files
        self._evaluated = self._crm_rows = 0     # denominators of rule_violation_pct

    def new_file(self, rel_path: str, source: str, scenarios: list[str], control: str = "none",
                 keep_labels: bool = False) -> FileLedger:
        ledger = FileLedger(self._batch, rel_path, source, sorted(set(scenarios)), control, keep_labels)
        self._files.append(ledger)
        return ledger

    def close_file(self, ledger: FileLedger, rows_written: int, status: str | None = None,
                   reason: str | None = None, after_fix: str | None = None) -> str:
        """Decides the file status (forced by the scenario or derived from the malformed ratio),
        finalises record outcomes, folds the file into the batch totals and returns the status."""
        if len(ledger.labels) != rows_written:
            raise AssertionError(f"{ledger.rel_path}: {len(ledger.labels)} labels for {rows_written} rows")
        if status is None:
            ratio = ledger.parse_failures / max(1, rows_written)
            status, reason = ((REJECTED, "MALFORMED_RATIO") if ratio > self._cfg.contract.max_malformed_ratio
                              else (LOADED, None))
        ledger.status, ledger.reason, ledger.after_fix = status, reason, after_fix
        for lab in ledger.labels:
            if status == REJECTED:
                lab.expected = FILE_REJECTED
            elif status == DUPLICATE:
                lab.expected = FILE_DUPLICATE
            else:
                self.keys.resolve(lab)
        self._account(ledger)
        if self._write and self._cfg.profile.record_labels:
            fh = self._labels()
            for i, lab in enumerate(ledger.labels, start=1):
                fh.write(json.dumps(lab.to_json(self._batch, ledger.rel_path, i), ensure_ascii=False,
                                    separators=(",", ":")).encode("utf-8") + b"\n")
        ledger.rows = len(ledger.labels)
        if not ledger.keep_labels:
            ledger.labels = []
        return status

    def _account(self, f: FileLedger) -> None:
        """Per-file expectation row and batch totals. Only loaded files contribute rows to silver."""
        loaded = f.status == LOADED
        hits: Counter = Counter()
        parse = dq = nullified = 0
        if loaded:
            for lab in f.labels:
                hits.update(lab.rules)
                parse += lab.expected == QUARANTINE_PARSE
                dq += lab.expected == QUARANTINE_DQ
                nullified += lab.record_class == "NULLIFY" and lab.expected != QUARANTINE_DQ
                self._rows["stale_ignored"] += lab.expected == STALE_IGNORED
                self._rows["duplicate_noop"] += lab.expected == DUPLICATE_NOOP
                if lab.source == "crm":
                    self._crm_rows += 1
                elif lab.source in ("helpdesk", "callcenter") and lab.expected != QUARANTINE_PARSE:
                    self._evaluated += 1           # denominators of the ticket DQ rule percentages
            hits.update(f.file_hits)
            self._hits.update(hits)
            self._rows["in_loaded_files"] += len(f.labels)
        self._rows["delivered"] += len(f.labels)
        self._rows["parse_quarantine"] += parse
        self._rows["dq_quarantine"] += dq
        self._rows["nullified"] += nullified
        f.expectation_row = [f.rel_path, f.source, f.status, f.reason or "", len(f.labels), parse, dq,
                             nullified, json.dumps(dict(sorted(hits.items())), separators=(",", ":")),
                             ";".join(f.scenarios), f.control, f.after_fix or ""]

    def end_batch(self, spec, crm_state: dict | None = None) -> dict:
        inserted, updated = self.keys.end_batch()
        pct = {}
        for rule, n in sorted(self._hits.items()):
            base = self._crm_rows if rule.startswith("R-CUS") else self._evaluated
            pct[rule] = round(100.0 * n / base, 3) if base else 0.0
        breaches = [r for r, limit in self._thresholds.items() if pct.get(r, 0.0) > limit]
        row_keys = ("delivered", "in_loaded_files", "parse_quarantine", "dq_quarantine", "nullified",
                    "stale_ignored", "duplicate_noop")
        summary = {
            "batch": spec.batch_id, "purpose": spec.purpose,
            "recommended_as_of": spec.recommended_as_of.isoformat() if spec.recommended_as_of else None,
            "files": {"total": len(self._files),
                      "by_status": dict(Counter(f.status for f in self._files)),
                      "rejected": {f.rel_path: f.reason for f in self._files if f.status == REJECTED},
                      "by_source": dict(Counter(f.source for f in self._files))},
            "rows": {k: self._rows[k] for k in row_keys},
            "rule_hits": dict(sorted(self._hits.items())),
            "rule_violation_pct": pct,
            "threshold_breaches": breaches,
            "expected_events": list(spec.expected_events),
        }
        if self.keys.enabled:
            summary["silver_ticket"] = {"inserted": inserted, "updated": updated, **self.keys.summary()}
        if crm_state is not None:
            summary["silver_customer"] = crm_state
        if self._write:
            self._write_batch(spec.batch_id, summary)
        self._batches.append(summary)
        return summary

    # ------------------------------------------------------------------ output
    def _write_batch(self, batch: str, summary: dict) -> None:
        out = self._layout.batch_dir(batch)
        out.mkdir(parents=True, exist_ok=True)
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow(FILE_COLUMNS)
        for f in self._files:
            w.writerow(f.expectation_row)
        (out / "file_expectations.csv").write_text(buf.getvalue(), encoding="utf-8")
        (out / "batch_expectations.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        self._close_labels()

    def _labels(self):
        if self._labels_fh is None:
            path = self._layout.labels(self._batch)
            path.parent.mkdir(parents=True, exist_ok=True)
            raw = open(path, "wb")
            # mtime=0 and no file name keep the .gz byte-identical between runs
            self._labels_fh = gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0, compresslevel=6)
            self._labels_raw = raw
        return self._labels_fh

    def _close_labels(self) -> None:
        if self._labels_fh is not None:
            self._labels_fh.close()
            self._labels_raw.close()
            self._labels_fh = None

    def add_golden(self, row: dict) -> bool:
        if len(self._golden) >= self._cfg.scenarios.golden_set_size:
            return False
        self._golden.append(row)
        return True

    def finalize(self, manifest: dict) -> None:
        self._close_labels()
        self._layout.dir.mkdir(parents=True, exist_ok=True)
        golden = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in self._golden)
        self._layout.golden.write_text(golden, encoding="utf-8")
        manifest["batches"] = [{k: b[k] for k in ("batch", "purpose", "recommended_as_of", "files")}
                               for b in self._batches]
        self._layout.manifest.write_text(json.dumps(manifest, indent=2, default=str) + "\n", encoding="utf-8")
