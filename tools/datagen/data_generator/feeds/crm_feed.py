"""CRM feed: one full customer snapshot per delivery (never split - a snapshot split across files
would break 'missing from the snapshot = inactive'). Implements S32."""
from __future__ import annotations

from datetime import date

from ..config.settings import GeneratorConfig
from ..core.rng import unit
from ..generators.customers import CustomerBase
from ..labels.outcomes import (ACCEPTED, ACCEPTED_NULLIFIED, CONFLICT_DUPLICATE, LOADED, QUARANTINE_DQ,
                               REJECTED, RecordLabel)
from ..labels.recorder import ExpectationRecorder
from ..model.reference import CRM_COLUMNS
from ..scenarios.plan import BatchSpec
from ..writers.files import CsvWriter
from ..writers.layout import LandingLayout


class CrmFeed:
    SOURCE = "crm"

    def __init__(self, cfg: GeneratorConfig, customers: CustomerBase, layout: LandingLayout,
                 recorder: ExpectationRecorder):
        self._cfg, self._customers, self._layout, self._rec = cfg, customers, layout, recorder
        self._latest_loaded: date | None = None

    def emit(self, spec: BatchSpec, write: bool) -> dict | None:
        delivery = spec.crm
        if delivery is None:
            return None
        path = self._layout.crm(delivery.snapshot_date, delivery.delivery)
        writer = CsvWriter(path, CRM_COLUMNS, write)
        ledger = self._rec.new_file(self._layout.relative(path), self.SOURCE,
                                    ["S32"] if delivery.partial else [])
        snapshot: dict[str, bool] = {}
        for values, label in self._rows(delivery.snapshot_no, delivery.partial):
            writer.write_row(values)
            ledger.add(label)
            if label.expected != QUARANTINE_DQ:
                snapshot[values[0]] = values[6] == "Y"
        stats = writer.close()
        status, reason = self._guard(delivery.snapshot_date, len(snapshot))
        status = self._rec.close_file(ledger, stats.rows, status, reason)
        if status == LOADED:
            for cid in self._rec.customers:
                if cid not in snapshot:
                    self._rec.customers[cid] = False           # missing from a full snapshot
            self._rec.customers.update(snapshot)
            self._latest_loaded = delivery.snapshot_date
        return {"snapshot_date": delivery.snapshot_date.isoformat(), "file_status": status,
                "rows_after": len(self._rec.customers), "active_after": sum(self._rec.customers.values())}

    def _guard(self, snapshot_date: date, valid_ids: int) -> tuple[str | None, str | None]:
        if self._latest_loaded is not None and snapshot_date < self._latest_loaded:
            return REJECTED, "STALE_SNAPSHOT"
        active = sum(self._rec.customers.values())
        if active and valid_ids < self._cfg.contract.snapshot_min_ratio_of_active * active:
            return REJECTED, "PARTIAL_SNAPSHOT"                                 # DV-8
        return None, None

    def _rows(self, snapshot_no: int, partial: bool):
        seed, sc = self._cfg.seed, self._cfg.scenarios
        limits = _cumulative(sc.crm_bad_id_share, sc.crm_blank_id_share, sc.crm_bad_tier_share,
                             sc.crm_bad_signup_share, sc.crm_duplicate_share)
        for c in self._customers.snapshot(snapshot_no):
            if partial and unit(seed, "crm", "partial", c.num) >= sc.crm_partial_ratio:
                continue
            values = [c.customer_id, c.name, c.tier, c.region, c.industry, c.signup_date.isoformat(),
                      "Y" if c.is_active else "N"]
            u = unit(seed, "crm", snapshot_no, c.num, "anomaly")
            label = RecordLabel(self.SOURCE, c.customer_id, snapshot_no, "VALID", ["S01"], [], ACCEPTED)
            if c.num <= self._cfg.skew.hot_customers:
                pass                                                # hot accounts stay clean
            elif u < limits[0]:
                values[0] = f"CUST-{c.num}"
                label = self._label(c.customer_id, snapshot_no, "DQ_ERROR", ["R-CUS-002"], QUARANTINE_DQ)
            elif u < limits[1]:
                values[0] = ""
                label = self._label(None, snapshot_no, "DQ_ERROR", ["R-CUS-001"], QUARANTINE_DQ)
            elif u < limits[2]:
                values[2] = "PLATINUM"
                label = self._label(c.customer_id, snapshot_no, "NULLIFY", ["R-CUS-003"], ACCEPTED_NULLIFIED)
            elif u < limits[3]:
                values[5] = "1899-12-31"                            # legacy spreadsheet default date
                label = self._label(c.customer_id, snapshot_no, "NULLIFY", ["R-CUS-004"], ACCEPTED_NULLIFIED)
            elif u < limits[4]:
                yield values, self._label(c.customer_id, snapshot_no, "VALID", [], ACCEPTED)
                dup = list(values)
                dup[1] = f"{values[1]} (old record)"
                label = self._label(c.customer_id, snapshot_no, "VALID", ["SYS-DUP-CONFLICT"],
                                    CONFLICT_DUPLICATE)
                values = dup
            yield values, label

    def _label(self, cid, snapshot_no, cls, rules, expected) -> RecordLabel:
        return RecordLabel(self.SOURCE, cid, snapshot_no, cls, ["S32"], rules, expected)


def _cumulative(*shares: float) -> list[float]:
    out, total = [], 0.0
    for s in shares:
        total += s
        out.append(total)
    return out
