"""Call-centre feed: one CSV per business day, delivered the next day at 01:30 UTC.
Implements S13/S14 (CSV damage), S20 (correction), S24+S27 (drifted header), S28 (MM/dd re-export)."""
from __future__ import annotations

import dataclasses
import random
from datetime import date, datetime, timedelta

from ..config.settings import GeneratorConfig
from ..core import timefmt as tf
from ..core.rng import Weighted, stream
from ..generators.callcenter import CallcenterSimulator
from ..generators.customers import CustomerBase
from ..generators.nested import ivr_features
from ..generators.text import TextFactory
from ..labels.outcomes import (ACCEPTED, ACCEPTED_SILENTLY_WRONG, DUPLICATE_NOOP, QUARANTINE_DQ,
                               RecordLabel, content_token, llm_expected, warn_hits)
from ..labels.recorder import ExpectationRecorder, callcenter_key
from ..model.records import Call
from ..model.reference import CALLCENTER_COLUMNS
from ..scenarios.mutators import CANONICAL_EQUIVALENT, VALID, CallcenterMutator, Mutation
from ..scenarios.plan import BatchSpec
from ..writers.files import CsvWriter
from ..writers.layout import LandingLayout

S27_ORDER = ("CALL_ID", "CALL_START", "CALL_END", "CUST_NO", "AGENT", "QUEUE", "PRIORITY_CD",
             "DISPOSITION", "HANDLE_SECS", "PRODUCT_LINE", "CSAT", "NOTES", "IVR_JSON")   # reordered
EXTRA_COLUMN = "WRAP_UP_CODE"                                                  # S24


class CallcenterFeed:
    SOURCE = "callcenter"

    def __init__(self, cfg: GeneratorConfig, customers: CustomerBase, text: TextFactory,
                 layout: LandingLayout, recorder: ExpectationRecorder):
        self._cfg, self._layout, self._rec = cfg, layout, recorder
        self._sim = CallcenterSimulator(cfg, customers, text)
        self._customers = customers
        self._mut = CallcenterMutator(cfg.anchor_date)
        self._mix = Weighted(cfg.mix.pairs())
        self._recent: dict[date, list[Call]] = {}          # clean calls of recent days, for S20

    def emit(self, spec: BatchSpec, write: bool) -> None:
        for business_date in spec.callcenter_days:
            delivery = tf.at(business_date + timedelta(days=1), 1, 30)
            self._emit_day(business_date, delivery, spec.callcenter_day_scenarios, write)
        for kind, delivery in spec.extras:
            if kind == "S20_CORRECTION":
                self._emit_correction(delivery, write)
            elif kind == "S28_BACKLOG":
                self._emit_backlog(delivery, spec.recommended_as_of, write)

    # ------------------------------------------------------------------ regular day file
    def _emit_day(self, business_date: date, delivery: datetime, scenarios: tuple[str, ...], write: bool):
        calls = self._sim.calls_for_day(business_date)
        self._recent = {d: v for d, v in self._recent.items() if d >= business_date - timedelta(days=2)}
        self._recent[business_date] = calls
        rng = stream(self._cfg.seed, "callcenter", "quality", business_date.isoformat())
        drift_extra = "S24" in scenarios
        columns = S27_ORDER if "S27" in scenarios else CALLCENTER_COLUMNS
        header = [c.lower() for c in columns] if "S27" in scenarios else list(columns)
        if drift_extra:
            header.append(EXTRA_COLUMN.lower() if "S27" in scenarios else EXTRA_COLUMN)
        path = self._layout.callcenter(business_date, delivery)
        writer = CsvWriter(path, header, write, bom="S27" in scenarios)
        ledger = self._rec.new_file(self._layout.relative(path), self.SOURCE, list(scenarios))
        dlv = tf.epoch_s(delivery)
        for call in calls:
            row = _row(call)
            cls = self._mix.pick(rng)
            if cls == "MALFORMED":
                line, mut = self._mut.malformed(rng, row, columns)
                writer.write_raw(line)
                ledger.add(self._label(call, mut, row, dlv, truncated=mut.detail == "TRUNCATED_ROW"))
                continue
            mut = self._mutate(rng, cls, row, call)
            values = [row[c] for c in columns] + ([rng.choice(("W01", "W07", "W12"))] if drift_extra else [])
            label = self._label(call, mut, row, dlv)
            label.scenarios = sorted(set(label.scenarios) | set(scenarios))
            if drift_extra and label.expected != "QUARANTINE_PARSE":
                label.rules.append("SYS-DRIFT-EXTRA")
            writer.write_row(values)
            ledger.add(label)
            if cls == "EXACT_DUPLICATE":                                   # S15: identical row again
                writer.write_row(values)
                copy = self._label(call, mut, row, dlv)
                copy.record_class, copy.scenarios, copy.expected = "EXACT_DUPLICATE", ["S15"], DUPLICATE_NOOP
                copy.rules = list(label.rules)
                ledger.add(copy)
        self._rec.close_file(ledger, writer.close().rows)

    def _mutate(self, rng: random.Random, cls: str, row: dict, call: Call) -> Mutation:
        if cls == "MILD":
            return self._mut.mild(rng, row, call.start)
        if cls == "NULLIFY":
            return self._mut.nullify(rng, row, call.start)
        if cls == "DQ_ERROR":
            return self._mut.dq_error(rng, row)
        return Mutation(VALID.record_class, list(VALID.scenarios), [], VALID.expected)

    def _label(self, call: Call, mut: Mutation, row: dict, dlv: int, truncated: bool = False) -> RecordLabel:
        cid = row["CALL_ID"].strip() or None
        notes = None if truncated else row["NOTES"]
        product = None if truncated else row["PRODUCT_LINE"]
        rules = list(mut.rules)
        if mut.expected != "QUARANTINE_PARSE":
            rules += warn_hits(row["CUST_NO"], product, notes, self._cfg.contract.max_description_chars)
        updated = mut.overrides.get("updated_at", call.end)
        scenarios = list(mut.scenarios) + [f for f in call.text.features]
        if not truncated:
            scenarios += ivr_features(row["IVR_JSON"])
        if self._customers.is_early(call.customer_id):
            scenarios.append("S37:EARLY_ARRIVING_CUSTOMER")
        return RecordLabel(
            self.SOURCE, cid, 1, mut.record_class, sorted(set(scenarios)), rules, mut.expected,
            llm_expected=None if truncated else llm_expected(None, notes), detail=mut.detail,
            key=callcenter_key(cid) if cid else None, upd=tf.epoch_s(updated), dlv=dlv,
            token=content_token(cid, "clean" if mut.detail in CANONICAL_EQUIVALENT else mut.detail))

    # ------------------------------------------------------------------ S20 correction
    def _emit_correction(self, delivery: datetime, write: bool) -> None:
        business_date = max(d for d in self._recent if d < delivery.date() - timedelta(days=1))
        calls = self._recent[business_date]
        rng = stream(self._cfg.seed, "callcenter", "correction", business_date.isoformat())
        count = max(10, round(len(calls) * self._cfg.scenarios.correction_share))
        picked = sorted(rng.sample(range(len(calls)), min(count, len(calls))))
        path = self._layout.callcenter(business_date, delivery)
        writer = CsvWriter(path, CALLCENTER_COLUMNS, write)
        ledger = self._rec.new_file(self._layout.relative(path), self.SOURCE, ["S20"])
        dlv = tf.epoch_s(delivery)
        for i in picked:
            call = calls[i]
            row = _row(call)
            row["CSAT"] = "4" if call.csat in (None, 5) else str(call.csat + 1)   # corrected survey score
            writer.write_row([row[c] for c in CALLCENTER_COLUMNS])
            label = self._label(call, Mutation("VALID", ["S20"], [], ACCEPTED, "CORRECTION"), row, dlv)
            ledger.add(label)
        self._rec.close_file(ledger, writer.close().rows)

    # ------------------------------------------------------------------ S28 re-export in MM/dd
    def _emit_backlog(self, delivery: datetime, as_of: datetime, write: bool) -> None:
        sc = self._cfg.scenarios
        daily = self._cfg.profile.callcenter_calls_per_day
        per_day = max(3, round(daily * sc.backlog_share / sc.backlog_days))
        first = delivery.date() - timedelta(days=sc.backlog_days)
        path = self._layout.callcenter(delivery.date() - timedelta(days=1), delivery)   # regular naming
        writer = CsvWriter(path, CALLCENTER_COLUMNS, write)
        ledger = self._rec.new_file(self._layout.relative(path), self.SOURCE, ["S28"])
        dlv = tf.epoch_s(delivery)
        limit = as_of + timedelta(hours=self._cfg.contract.future_tolerance_hours)
        for offset in range(sc.backlog_days):
            for call in self._sim.backlog_calls(first + timedelta(days=offset), per_day):
                row = _row(call)
                row["CALL_START"], row["CALL_END"] = tf.ist_mdy(call.start), tf.ist_mdy(call.end)
                writer.write_row([row[c] for c in CALLCENTER_COLUMNS])
                ledger.add(self._backlog_label(call, row, dlv, limit))
        self._rec.close_file(ledger, writer.close().rows)

    def _backlog_label(self, call: Call, row: dict, dlv: int, limit: datetime) -> RecordLabel:
        """What dd/MM parsing does to an MM/dd value: day > 12 cannot parse (NULL created and
        updated), day <= 12 swaps day and month - future dates are caught, past ones load wrongly."""
        local = call.start.astimezone(tf.IST)
        mut = Mutation("VALID", ["S28"], [], ACCEPTED_SILENTLY_WRONG, "MMDD_SWAPPED")
        updated = call.end
        if local.day > 12:
            mut = Mutation("VALID", ["S28"], ["R-TKT-002", "R-TKT-003"], QUARANTINE_DQ, "MMDD_UNPARSEABLE")
        else:
            swapped = _swap(call.start)
            updated = _swap(call.end)
            if swapped > limit:
                mut = Mutation("VALID", ["S28"], ["R-TKT-007"], QUARANTINE_DQ, "MMDD_SWAPPED_FUTURE")
        mut = dataclasses.replace(mut, overrides={"updated_at": updated})
        return self._label(call, mut, row, dlv)


def _swap(ts: datetime) -> datetime:
    local = ts.astimezone(tf.IST)
    swapped = datetime(local.year, local.day, local.month, local.hour, local.minute, tzinfo=tf.IST)
    return swapped.astimezone(tf.UTC)


def _row(call: Call) -> dict[str, str]:
    return {"CALL_ID": call.call_id, "CUST_NO": call.customer_id or "", "CALL_START": tf.ist_dmy(call.start),
            "CALL_END": tf.ist_dmy(call.end), "AGENT": call.agent, "QUEUE": call.queue,
            "PRIORITY_CD": call.priority, "DISPOSITION": call.disposition,
            "HANDLE_SECS": str(call.handle_secs),
            "NOTES": call.text.description, "PRODUCT_LINE": call.product,
            "CSAT": "" if call.csat is None else str(call.csat), "IVR_JSON": call.ivr}
