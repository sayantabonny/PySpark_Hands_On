"""Helpdesk feed: 4-hourly JSONL exports with control files.

For every export: render each snapshot as the source would, apply its record-quality class,
move a slice into the export's scenario file when the plan says so (S22-S27), inject S18
conflicts, split into files of at most max_records_per_file lines, then write data + control.
Extra deliveries: S17 late versions, S19 byte-identical copies, B005 corrected re-deliveries.
"""
from __future__ import annotations

import dataclasses
import logging
import random
from dataclasses import dataclass
from datetime import datetime

from ..config.settings import GeneratorConfig
from ..core import timefmt as tf
from ..core.rng import Weighted, chance, stream
from ..generators.customers import CustomerBase
from ..generators.helpdesk import HelpdeskSimulator
from ..generators.nested import NestedBuilder, helpdesk_features
from ..generators.text import TextFactory
from ..labels.outcomes import (ACCEPTED, CONFLICT_DUPLICATE, DUPLICATE, DUPLICATE_NOOP, LOADED,
                               QUARANTINE_PARSE, REJECTED, RecordLabel, content_token, llm_expected,
                               warn_hits)
from ..labels.recorder import ExpectationRecorder, FileLedger
from ..model.records import Snapshot
from ..model.reference import HD_PRIORITY, HD_STATUS, HOT_PRODUCT, PLACEHOLDER_CUSTOMER, agent_id
from ..scenarios.mutators import CANONICAL_EQUIVALENT, VALID, HelpdeskMutator, Mutation, corrupt_json_line
from ..scenarios.plan import BatchSpec
from ..writers.files import FileStats, JsonlWriter, encode_json, write_bytes, write_control
from ..writers.layout import LandingLayout

log = logging.getLogger(__name__)
FILE_SCENARIOS = ("S22", "S23A", "S23B", "S25", "S26", "S26B", "S24", "S27")
REDELIVERED = ("S22", "S23A", "S23B", "S25")
COPY_SOURCES = {("B002", 4): "B002", ("B003", 20): "B003"}       # (batch, export hour) of seq 0001
FORCED_STATUS = {"S23A": (REJECTED, "CONTROL_MISMATCH", None),
                 "S23B": (REJECTED, "CONTROL_FILE_MISSING", None),
                 "S25": (REJECTED, "MISSING_REQUIRED_FIELD:priority", None),
                 "S26": (REJECTED, "MISSING_REQUIRED_FIELD:created_at",
                         "LOADED after alias createdAt->created_at and REPROCESS")}


@dataclass
class _Pending:
    snap: Snapshot
    rec: dict
    mut: Mutation
    label: RecordLabel
    corrupt: bool = False
    dup: str | None = None          # exact-duplicate copy: 'same' file or 'carry' to the next export
    clean: dict | None = None       # the record before file-level damage (for B005 re-delivery)


class HelpdeskFeed:
    SOURCE = "helpdesk"

    def __init__(self, cfg: GeneratorConfig, customers: CustomerBase, text: TextFactory,
                 layout: LandingLayout, recorder: ExpectationRecorder):
        self._cfg, self._layout, self._rec = cfg, layout, recorder
        self._customers = customers
        self._sim = HelpdeskSimulator(cfg, customers, text)
        self._nested = NestedBuilder(cfg.nested, cfg.seed)
        self._exports = self._sim.exports()
        self._peek: tuple[datetime, list[Snapshot]] | None = None
        self._mut = HelpdeskMutator(cfg.anchor_date)
        self._mix = Weighted(cfg.mix.pairs())
        self._daily = cfg.profile.helpdesk_tickets_per_day
        self._carry: list[tuple[bytes, RecordLabel]] = []
        self._late_target = max(20, round(self._daily * cfg.scenarios.late_versions_share))
        self._pool: list[Snapshot] = []
        self._pool_seen = 0
        self._pool_rng = stream(cfg.seed, "helpdesk", "late_pool")
        self._copies: dict[str, tuple[FileStats, FileStats, FileLedger]] = {}
        self._rejected: list[tuple[str, list[_Pending]]] = []
        self._files_written = 0

    # ================================================================== batch entry point
    def emit(self, spec: BatchSpec, write: bool) -> None:
        while spec.helpdesk_until is not None:
            export = self._next_export(spec.helpdesk_until)
            if export is None:
                break
            self._emit_export(spec, export[0], export[1], write)
        for kind, delivery in spec.extras:
            if kind == "S17_LATE_FILE":
                self._emit_late_versions(delivery, write)
            elif kind in ("S19_COPY", "S19_COPY_B006"):
                self._emit_copy("B002" if kind == "S19_COPY" else "B003", delivery, write)
            elif kind == "REDELIVERY":
                self._emit_redelivery(delivery, write)

    def _next_export(self, until: datetime):
        if self._peek is None:
            self._peek = next(self._exports, None)
        if self._peek is None or self._peek[0] > until:
            return None
        export, self._peek = self._peek, None
        return export

    # ================================================================== one export
    def _emit_export(self, spec: BatchSpec, delivery: datetime, snaps: list[Snapshot], write: bool) -> None:
        rng = stream(self._cfg.seed, "helpdesk", "export", tf.compact(delivery))
        scenario = spec.helpdesk_slot_scenarios.get(delivery.hour)
        n_scen = 0
        if scenario in FILE_SCENARIOS:
            n_scen = min(len(snaps), max(self._cfg.scenarios.scenario_file_min_records,
                                         round(len(snaps) * self._cfg.scenarios.scenario_file_share)))
        main_snaps, scen_snaps = snaps[:len(snaps) - n_scen], snaps[len(snaps) - n_scen:]
        if spec.batch_id in ("B001", "B002"):
            for snap in snaps:
                self._offer_late_pool(snap)
        main = [self._render(rng, s, delivery, None, spec.batch_id) for s in main_snaps]
        scen = [self._render(rng, s, delivery, scenario, spec.batch_id) for s in scen_snaps]
        pairs = self._inject_conflicts(rng, main) if scenario == "S18" else 0

        carried, self._carry = self._carry, []
        chunks = _chunks(main, self._cfg.profile.max_records_per_file, len(carried))
        sc = self._cfg.scenarios
        for seq, chunk in enumerate(chunks, start=1):
            self._files_written += 1
            forced, forced_sid = 0.0, None
            if spec.s21_mode == "every_nth" and self._files_written % sc.s21_every_nth_file == 0:
                forced, forced_sid = sc.s21_malformed_ratio, "S21"
            elif spec.s21_mode == "one" and delivery.hour == 12 and seq == 1:
                forced, forced_sid = sc.s21_malformed_ratio, "S21"
            self._write_file(spec, delivery, seq, chunk, write, scenario=None,
                             carried=carried if seq == 1 else [], forced=forced, forced_sid=forced_sid,
                             conflict_pairs=pairs if seq == 1 else 0)
        if scen:
            ratio = sc.s22_malformed_ratio if scenario == "S22" else 0.0
            self._write_file(spec, delivery, len(chunks) + 1, scen, write, scenario=scenario,
                             forced=ratio, forced_sid="S22" if ratio else None)

    # ------------------------------------------------------------------ rendering
    def _clean_record(self, snap: Snapshot) -> dict:
        t = snap.ticket
        fmt = lambda dt: tf.iso(dt, t.ts_offset)                   # noqa: E731
        status = snap.pending_label if snap.status == "PENDING" else HD_STATUS[snap.status]
        tags = self._nested.damage_tags((t.num, snap.version_no), list(t.tags))
        return {"ticket_id": t.ticket_id, "created_at": fmt(t.created_at), "updated_at": fmt(snap.updated_at),
                "resolved_at": fmt(snap.resolved_at) if snap.resolved_at else None, "status": status,
                "priority": HD_PRIORITY[t.priority], "channel": t.channel, "product": t.product,
                "subject": t.text.subject, "description": t.text.description,
                "requester": {"customer_id": t.customer_id, "email": t.email, "locale": t.locale},
                "assignee_id": snap.assignee, "tags": tags, "category": t.agent_category,
                "reopen_count": snap.reopen_count, "csat": snap.csat,
                **self._nested.helpdesk_fields(snap), "schema_version": "1"}

    def _render(self, rng: random.Random, snap: Snapshot, delivery: datetime, scenario: str | None,
                batch: str) -> _Pending:
        rec = self._clean_record(snap)
        cls = self._mix.pick(rng)
        if cls == "MALFORMED" and scenario:     # a 40-line scenario file must not cross 5% by chance
            cls = "VALID"
        t = snap.ticket
        if cls == "MILD":
            mut = self._mut.mild(rng, rec, t.created_at, snap.updated_at, snap.resolved_at)
        elif cls == "NULLIFY":
            mut = self._mut.nullify(rng, rec, t.created_at)
        elif cls == "DQ_ERROR":
            mut = self._mut.dq_error(rng, rec)
        else:
            mut = dataclasses.replace(VALID, scenarios=list(VALID.scenarios))
        clean = {**rec, "requester": dict(rec["requester"])} if scenario in REDELIVERED else None
        extra_rules = self._file_transform(rng, rec, scenario)
        if extra_rules is None:                                   # S27: this record's csat became text
            extra_rules, mut = ["SYS-CAST"], dataclasses.replace(mut, detail="CSAT_TEXT")
        pending = _Pending(snap, rec, mut, self._label(snap, rec, mut, delivery, extra_rules, scenario),
                           corrupt=cls == "MALFORMED", clean=clean)
        if cls == "EXACT_DUPLICATE" and not scenario:
            pending.dup = "same" if chance(rng, 0.5) else "carry"
            pending.label.scenarios = sorted(set(pending.label.scenarios) | {"S15"})
        if (batch == "B001" and cls == "VALID" and snap.version_no == 1 and not scenario and t.num % 97 == 0
                and not t.text.fault and not any(f.startswith(("S30", "S29")) for f in t.text.features)):
            self._rec.add_golden({
                "ticket_key": f"helpdesk:{t.ticket_id}", "intended_category": t.text.category,
                "intended_sentiment": t.text.sentiment, "intended_urgency": t.text.urgency,
                "language": t.text.language, "templated": "S33:TEMPLATED" in t.text.features})
        return pending

    def _file_transform(self, rng: random.Random, rec: dict, scenario: str | None) -> list[str] | None:
        """File-level drift (S24-S27). Returns extra rule hits, or None for an S27 csat-as-text record."""
        if scenario == "S26B" and "description" in rec:
            rec["desc"] = rec.pop("description")
            return ["SYS-DRIFT-EXTRA"]
        if scenario == "S24":
            rec["sentiment_hint"] = rng.choice(("negative", "neutral", "positive"))
            rec["schema_version"] = "2"
            return ["SYS-DRIFT-EXTRA"]
        share = self._cfg.scenarios.s27_csat_text_share
        if scenario == "S27" and rec.get("csat") is not None and chance(rng, share):
            rec["csat"] = f"{rec['csat']}/5"
            return None
        if scenario == "S25":
            rec.pop("priority", None)
        elif scenario == "S26" and "created_at" in rec:
            rec["createdAt"] = rec.pop("created_at")
        return []

    def _label(self, snap: Snapshot, rec: dict, mut: Mutation, delivery: datetime, extra_rules: list[str],
               scenario: str | None) -> RecordLabel:
        t = snap.ticket
        tid = rec.get("ticket_id")
        sid = tid.strip() if isinstance(tid, str) and tid.strip() else None
        description = rec.get("description")
        rules = list(mut.rules) + extra_rules + warn_hits(
            rec["requester"]["customer_id"], rec.get("product"), description,
            self._cfg.contract.max_description_chars)
        scenarios = set(mut.scenarios) | set(t.text.features) | set(helpdesk_features(rec))
        if scenario:
            scenarios.add(scenario.rstrip("AB") if scenario in ("S23A", "S23B") else scenario)
        if snap.version_no > 1:
            scenarios.add("S16")
        if t.mega:
            scenarios.add("S29:MEGA_TICKET")
        if self._customers.is_early(t.customer_id):
            scenarios.add("S37:EARLY_ARRIVING_CUSTOMER")
        if t.customer_id == PLACEHOLDER_CUSTOMER:
            scenarios.add("S29:PLACEHOLDER_CUSTOMER")
        if t.product == HOT_PRODUCT:
            scenarios.add("S29:HOT_PRODUCT")
        if "R-TKT-016" in rules:
            scenarios.add("S10")
        token_detail = "clean" if mut.detail in CANONICAL_EQUIVALENT else mut.detail
        if scenario == "S26B":
            token_detail = f"{token_detail}:NODESC"
        return RecordLabel(self.SOURCE, sid, snap.version_no, mut.record_class, sorted(scenarios), rules,
                           mut.expected, llm_expected=llm_expected(rec.get("subject"), description),
                           detail=mut.detail, key=t.num if sid else None, upd=tf.epoch_s(snap.updated_at),
                           dlv=tf.epoch_s(delivery),
                           token=content_token(t.num, snap.version_no, token_detail))

    def _inject_conflicts(self, rng: random.Random, pendings: list[_Pending]) -> int:
        """S18: a second rendering of the same version (same key, updated_at and delivery) with a
        different assignee. The pipeline keeps one of the two (hash tie-break) and flags the pair."""
        target = max(10, round(self._daily * self._cfg.scenarios.conflict_pairs_share))
        eligible = [i for i, p in enumerate(pendings)
                    if p.label.record_class == "VALID" and p.label.key is not None and not p.dup]
        chosen = sorted(rng.sample(eligible, min(target, len(eligible))), reverse=True)
        for i in chosen:
            p = pendings[i]
            other = agent_id(1 + p.snap.ticket.num % 250)
            if other == p.rec.get("assignee_id"):
                other = agent_id(2 + p.snap.ticket.num % 250)
            rec = {**p.rec, "requester": dict(p.rec["requester"]), "assignee_id": other}
            label = dataclasses.replace(p.label, scenarios=sorted(set(p.label.scenarios) | {"S18"}),
                                        expected=CONFLICT_DUPLICATE, rules=list(p.label.rules),
                                        token=content_token(p.snap.ticket.num, p.snap.version_no, "VARIANT"))
            p.label.scenarios = sorted(set(p.label.scenarios) | {"S18"})
            pendings.insert(i + 1, _Pending(p.snap, rec, p.mut, label))
        return len(chosen)

    # ------------------------------------------------------------------ writing
    def _write_file(self, spec: BatchSpec, delivery: datetime, seq: int, pendings: list[_Pending],
                    write: bool, scenario: str | None, carried: list[tuple[bytes, RecordLabel]] = (),
                    forced: float = 0.0, forced_sid: str | None = None, conflict_pairs: int = 0) -> None:
        path = self._layout.helpdesk(delivery, seq)
        copy_source = COPY_SOURCES.get((spec.batch_id, delivery.hour)) if seq == 1 and not scenario else None
        writer = JsonlWriter(path, write, keep_bytes=copy_source is not None)
        control = {"S23A": "mismatch", "S23B": "missing"}.get(scenario, "present")
        ledger = self._rec.new_file(self._layout.relative(path), self.SOURCE,
                                    [s for s in (scenario, forced_sid) if s], control,
                                    keep_labels=copy_source is not None)
        if conflict_pairs:
            ledger.file_hits["SYS-DUP-CONFLICT"] += conflict_pairs
        rng = stream(self._cfg.seed, "helpdesk", "corrupt", tf.compact(delivery), seq)
        forced_idx = self._forced_indices(rng, pendings, forced)
        dlv = tf.epoch_s(delivery)
        for line, label in carried:                                    # S15 copies from the previous export
            writer.write_line(line)
            ledger.add(dataclasses.replace(label, dlv=dlv))
        for i, p in enumerate(pendings):
            line = encode_json(p.rec)
            if p.corrupt or i in forced_idx:
                line, detail = corrupt_json_line(rng, line)
                p.label.record_class, p.label.expected, p.label.detail = "MALFORMED", QUARANTINE_PARSE, detail
                p.label.rules, p.label.key = ["SYS-PARSE"], None
                extra = {forced_sid} if i in forced_idx else set()
                p.label.scenarios = sorted(set(p.label.scenarios) | {"S14"} | extra)
            writer.write_line(line)
            ledger.add(p.label)
            if p.dup:
                copy = dataclasses.replace(p.label, record_class="EXACT_DUPLICATE", scenarios=["S15"],
                                           expected=DUPLICATE_NOOP, rules=list(p.label.rules))
                if p.dup == "same":
                    writer.write_line(line)
                    ledger.add(copy)
                else:
                    self._carry.append((line, copy))
        stats = writer.close()
        ctl = None
        if scenario != "S23B":
            ctl = write_control(stats, delivery, "2" if scenario == "S24" else "1", write,
                                count_delta=self._cfg.scenarios.s23_count_delta if scenario == "S23A" else 0)
        status, reason, after_fix = FORCED_STATUS.get(scenario, (None, None, None))
        status = self._rec.close_file(ledger, stats.rows, status, reason, after_fix)
        if copy_source:
            self._copies[copy_source] = (stats, ctl, ledger)
        if scenario in REDELIVERED:
            self._rejected.append((scenario, pendings))

    @staticmethod
    def _forced_indices(rng: random.Random, pendings: list[_Pending], ratio: float) -> set[int]:
        if not ratio or not pendings:
            return set()
        already = sum(1 for p in pendings if p.corrupt)
        candidates = [i for i, p in enumerate(pendings) if not p.corrupt and not p.dup]
        extra = max(0, round(ratio * len(pendings)) - already)
        return set(rng.sample(candidates, min(extra, len(candidates))))

    # ================================================================== extra deliveries
    def _offer_late_pool(self, snap: Snapshot) -> None:
        """Reservoir sample of older snapshots, re-delivered late in B003 (S17)."""
        capacity = 5 * self._late_target
        self._pool_seen += 1
        if len(self._pool) < capacity:
            self._pool.append(snap)
        else:
            j = self._pool_rng.randrange(self._pool_seen)
            if j < capacity:
                self._pool[j] = snap

    def _emit_late_versions(self, delivery: datetime, write: bool) -> None:
        keys = self._rec.keys
        candidates = sorted(self._pool, key=lambda s: (s.ticket.num, s.version_no))
        if keys.enabled:
            candidates = [s for s in candidates
                          if (keys.newest_updated(s.ticket.num) or 0) > tf.epoch_s(s.updated_at)]
        chosen = candidates[:self._late_target]
        pendings = []
        for snap in chosen:
            rec = self._clean_record(snap)
            mut = Mutation("VALID", ["S17"], [], ACCEPTED, "LATE_OLDER_VERSION")
            pendings.append(_Pending(snap, rec, mut, self._label(snap, rec, mut, delivery, [], "S17")))
        self._write_extra_file(delivery, 1, pendings, write, ["S17"])

    def _emit_copy(self, source_batch: str, delivery: datetime, write: bool) -> None:
        if source_batch not in self._copies:
            log.warning("S19 copy skipped: source file of %s was not generated", source_batch)
            return
        stats, ctl, source = self._copies[source_batch]
        if source.status != LOADED:
            log.warning("S19 copy skipped: source file %s was not loaded", source.rel_path)
            return
        path = self._layout.helpdesk(delivery, 1)
        copy = write_bytes(path, stats.content, stats.rows, write)
        write_bytes(self._layout.control_for(path), ctl.content, 1, write)
        ledger = self._rec.new_file(self._layout.relative(path), self.SOURCE, ["S19"], "present")
        for lab in source.labels:
            ledger.add(dataclasses.replace(lab, scenarios=sorted(set(lab.scenarios) | {"S19"})))
        self._rec.close_file(ledger, copy.rows, DUPLICATE, f"SAME_CONTENT_AS:{source.rel_path}")

    def _emit_redelivery(self, delivery: datetime, write: bool) -> None:
        """The source re-exports the rejected B004 files, fixed, with a new delivery timestamp."""
        for seq, (scenario, pendings) in enumerate(self._rejected, start=1):
            fixed = []
            for p in pendings:
                rec = p.clean if p.clean is not None else p.rec
                mut = p.mut if not p.corrupt else dataclasses.replace(VALID, scenarios=list(VALID.scenarios))
                label = self._label(p.snap, rec, mut, delivery, [], None)
                label.scenarios = sorted(set(label.scenarios) | {scenario[:3]})
                fixed.append(_Pending(p.snap, rec, mut, label))
            self._write_extra_file(delivery, seq, fixed, write, [f"{scenario}-FIXED"])

    def _write_extra_file(self, delivery: datetime, seq: int, pendings: list[_Pending], write: bool,
                          scenarios: list[str]) -> None:
        path = self._layout.helpdesk(delivery, seq)
        writer = JsonlWriter(path, write)
        ledger = self._rec.new_file(self._layout.relative(path), self.SOURCE, scenarios, "present")
        for p in pendings:
            writer.write_line(encode_json(p.rec))
            ledger.add(p.label)
        stats = writer.close()
        write_control(stats, delivery, "1", write)
        self._rec.close_file(ledger, stats.rows)


def _chunks(pendings: list[_Pending], size: int, first_offset: int) -> list[list[_Pending]]:
    """Splits an export into files of at most `size` lines; S15 copies count as lines."""
    chunks: list[list[_Pending]] = []
    current: list[_Pending] = []
    lines = first_offset                               # carried copies go into the first file
    for p in pendings:
        need = 2 if p.dup == "same" else 1
        if current and lines + need > size:
            chunks.append(current)
            current, lines = [], 0
        current.append(p)
        lines += need
    chunks.append(current)
    return chunks
