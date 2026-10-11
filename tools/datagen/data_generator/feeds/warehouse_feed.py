"""Data-modelling sources for CDC, SCD, star/snowflake and Data Vault practice.

* S35  CRM customer-profile change feed (JSONL, Debezium-like): one initial snapshot file (op 'r')
       and one file per business day of 'c' / 'u' / 'd' events with source_seq, commit_ts,
       effective_from, changed_fields and before/after images.
* S36  Reference dimensions: product catalog snapshots (product -> family -> business unit) and an
       effective-dated agent roster (agent -> team -> site) delivered as full-history extracts.
* S37  Keys and identity: billing account key map (billing_account_id -> customer_id, valid_from/to)
       plus early-arriving facts (tickets/calls naming customers CRM delivers later, see CustomerBase).

Deliveries are assigned to batches by their delivery time (window = previous batch's recommended
as-of, this batch's recommended as-of]. Everything is derived from dedicated random streams.
Ground truth for the CDC feed is written to <expected>/<profile>/warehouse/.
"""
from __future__ import annotations

import csv
import io
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from ..config.settings import GeneratorConfig
from ..core import timefmt as tf
from ..core.rng import chance, stream, unit
from ..generators.customers import CustomerBase
from ..labels.outcomes import ACCEPTED, DUPLICATE, DUPLICATE_NOOP, QUARANTINE_PARSE, RecordLabel
from ..labels.recorder import ExpectationRecorder
from ..model.reference import CALLCENTER_AGENTS, HELPDESK_AGENTS, PRODUCTS, UNKNOWN_PRODUCTS, agent_id, customer_id
from ..scenarios.plan import BatchSpec
from ..writers.files import CsvWriter, JsonlWriter, encode_json, write_bytes
from ..writers.layout import ExpectedLayout, LandingLayout

# ------------------------------------------------------------------ CDC vocabulary
SEGMENTS = ("SMB", "MID_MARKET", "ENTERPRISE")
CITIES = ("Bengaluru", "Mumbai", "Singapore", "London", "Berlin", "New York", "Austin", "Sao Paulo", "Sydney",
          "Toronto")
ACCOUNT_MANAGERS = tuple(f"AM-{i:03d}" for i in range(1, 41))
CREDIT_LIMITS = (5_000, 10_000, 25_000, 50_000, 100_000, 250_000)
STATUSES = ("ACTIVE", "ON_HOLD", "CHURN_RISK")
PROFILE_FIELDS = ("segment", "account_manager", "city", "credit_limit", "status")

# ------------------------------------------------------------------ product hierarchy
FAMILIES = {   # family_code -> (family_name, business_unit)
    "IDENTITY": ("Identity & Access", "Core Platform"), "PLATFORM": ("Developer Platform", "Core Platform"),
    "MOBILE": ("Mobile Apps", "Core Platform"), "BILLING": ("Billing & Payments", "Commerce"),
    "DATA": ("Data & Analytics", "Data & Integration"), "INTEGRATIONS": ("Integrations", "Data & Integration"),
    "MESSAGING": ("Messaging", "Engagement"), "WORKFLOW": ("Workflow & Content", "Engagement"),
}
PRODUCT_FAMILY = {
    "LOGIN-SSO": "IDENTITY", "IDENTITY-MFA": "IDENTITY", "USER-ADMIN": "IDENTITY",
    "API-GATEWAY": "PLATFORM", "WEB-CONSOLE": "PLATFORM", "CLI-TOOLS": "PLATFORM", "SDK-PYTHON": "PLATFORM",
    "SDK-JAVA": "PLATFORM", "SANDBOX": "PLATFORM", "STATUS-PAGE": "PLATFORM",
    "MOBILE-IOS": "MOBILE", "MOBILE-ANDROID": "MOBILE",
    "BILLING-PORTAL": "BILLING", "PAYMENTS": "BILLING", "LICENSING": "BILLING", "MARKETPLACE": "BILLING",
    "REPORTING": "DATA", "DATA-EXPORT": "DATA", "DATA-IMPORT": "DATA", "ANALYTICS": "DATA", "DASHBOARDS": "DATA",
    "SEARCH": "DATA", "AUDIT-LOG": "DATA", "BACKUP": "DATA",
    "INTEGRATIONS-HUB": "INTEGRATIONS", "CRM-CONNECTOR": "INTEGRATIONS", "ERP-CONNECTOR": "INTEGRATIONS",
    "WEBHOOKS": "INTEGRATIONS", "CALENDAR-SYNC": "INTEGRATIONS",
    "NOTIFICATIONS": "MESSAGING", "EMAIL-DELIVERY": "MESSAGING", "SMS-GATEWAY": "MESSAGING",
    "CHAT-WIDGET": "MESSAGING", "ALERTING": "MESSAGING",
    "WORKFLOW-ENGINE": "WORKFLOW", "SCHEDULER": "WORKFLOW", "FILE-STORAGE": "WORKFLOW", "ONBOARDING": "WORKFLOW",
    "KNOWLEDGE-BASE": "WORKFLOW", "TRANSLATION": "WORKFLOW",
}
assert set(PRODUCT_FAMILY) == set(PRODUCTS), "every product needs a family"
PRODUCT_COLUMNS = ("product_code", "product_name", "family_code", "family_name", "business_unit", "launch_date",
                   "retired_date", "is_active")

# ------------------------------------------------------------------ agent roster
HD_TEAMS = ("HD-ACCESS", "HD-BILLING", "HD-TECH-1", "HD-TECH-2", "HD-GENERAL")
CC_TEAMS = ("CC-VOICE-A", "CC-VOICE-B", "CC-ESCALATIONS")
SITES = ("Bengaluru", "Pune", "Manila", "Krakow", "Austin")
FIRST_NAMES = ("Asha", "Ravi", "Maria", "John", "Wei", "Fatima", "Lucas", "Priya", "Tom", "Elena", "Kenji", "Sara",
               "Omar", "Nina", "Arjun", "Grace")
LAST_NAMES = ("Kumar", "Silva", "Smith", "Chen", "Khan", "Novak", "Reyes", "Iyer", "Brown", "Rossi", "Sato", "Ali")
MISSING_AGENTS = (137, 242, 388)                # assigned to tickets/calls but absent from the roster
AGENT_COLUMNS = ("agent_id", "agent_name", "team", "site", "role", "effective_from", "effective_to")

# ------------------------------------------------------------------ billing
BILLING_COLUMNS = ("billing_account_id", "customer_id", "account_type", "currency", "payment_terms",
                   "valid_from", "valid_to")
CURRENCIES = ("USD", "EUR", "INR", "GBP", "SGD")
PAYMENT_TERMS = ("NET15", "NET30", "NET45", "NET60")


@dataclass
class _Delivery:
    at: datetime
    source: str
    write_fn: object                     # callable(write: bool) -> None
    done: bool = False


@dataclass
class _Event:
    seq: int
    day: date | None                     # business day (None = initial snapshot)
    body: dict
    feats: list[str] = field(default_factory=list)
    late: bool = False


class WarehouseFeed:
    def __init__(self, cfg: GeneratorConfig, customers: CustomerBase, layout: LandingLayout,
                 recorder: ExpectationRecorder):
        self._cfg, self._customers, self._layout, self._rec = cfg, customers, layout, recorder
        self._w = cfg.warehouse
        self._seed = cfg.seed
        self._anchor = cfg.anchor_date
        self._start = cfg.anchor_date - timedelta(days=cfg.profile.days - 1)
        self._prev_as_of: datetime | None = None
        self._kept: dict[str, tuple] = {}
        self._deliveries: list[_Delivery] = []
        if "crm_cdc" in cfg.sources:
            self._events = self._simulate_cdc()
            self._plan_cdc()
        if "products" in cfg.sources:
            self._plan_products()
        if "agents" in cfg.sources:
            self._plan_agents()
        if "billing" in cfg.sources:
            self._plan_billing()
        self._deliveries.sort(key=lambda d: (d.at, d.source))

    # ================================================================== batch entry point
    def emit(self, spec: BatchSpec, write: bool) -> None:
        as_of = spec.recommended_as_of
        for d in self._deliveries:
            if not d.done and (self._prev_as_of is None or d.at > self._prev_as_of) and d.at <= as_of:
                d.write_fn(write)
                d.done = True
        self._prev_as_of = as_of

    def _day(self, n: int) -> date:
        return self._anchor + timedelta(days=n)

    # ================================================================== S35 CDC feed
    def _profile(self, rng: random.Random) -> dict:
        return {"segment": rng.choice(SEGMENTS), "account_manager": rng.choice(ACCOUNT_MANAGERS),
                "city": rng.choice(CITIES), "credit_limit": rng.choice(CREDIT_LIMITS), "status": "ACTIVE"}

    @staticmethod
    def _new_value(rng: random.Random, fld: str, old):
        options = {"segment": SEGMENTS, "account_manager": ACCOUNT_MANAGERS + (None,), "city": CITIES,
                   "credit_limit": CREDIT_LIMITS, "status": STATUSES}[fld]
        return rng.choice([o for o in options if o != old])

    def _simulate_cdc(self) -> list[_Event]:
        w, n = self._w, self._customers.base_size
        rng = stream(self._seed, "cdc", "simulation")
        events: list[_Event] = []
        seq = 0

        def emit(op, cid, ts, before, after, eff, day, feats=()):
            nonlocal seq
            seq += 1
            if op == "u":
                changed = sorted(k for k in PROFILE_FIELDS if before.get(k) != after.get(k))
            elif op in ("c", "r"):
                changed = list(PROFILE_FIELDS)
            else:
                changed = []
            body = {"event_id": f"EV{seq:09d}", "op": op, "source_seq": seq, "commit_ts": tf.iso_millis(ts),
                    "customer_id": cid, "effective_from": eff.isoformat(), "changed_fields": changed,
                    "before": dict(before) if before is not None else None,
                    "after": dict(after) if after is not None else None}
            ev = _Event(seq, day, body, list(feats))
            events.append(ev)
            return ev

        hot = self._cfg.skew.hot_customers
        state: dict[str, dict] = {}
        initial_ts = tf.at(self._start) - timedelta(hours=2)
        for num in range(1, n + 1):
            if num <= hot or unit(self._seed, "cdc", "has", num) < w.cdc_profile_share:
                cid = customer_id(num)
                state[cid] = self._profile(rng)
                eff = self._start - timedelta(days=30 + int(unit(self._seed, "cdc", "since", num) * 900))
                emit("r", cid, initial_ts, None, state[cid], eff, None)

        reinserts: dict[date, list[str]] = {}
        crm_new: dict[date, list[str]] = {}
        for k in (1, 2, 3):
            crm_new.setdefault(self._customers.signup_of_added(k), []).extend(
                customer_id(num) for num in self._customers.added_in(k))
        day = self._start
        while day <= self._anchor:
            active = sorted(state)
            actions = (["update"] * round(len(active) * w.cdc_update_share)
                       + ["delete"] * max(1, round(len(active) * w.cdc_delete_share))
                       + ["noop"] * w.cdc_noop_updates_per_day + ["same_ts"] * w.cdc_same_ts_pairs_per_day
                       + ["backdated"] * w.cdc_backdated_per_day + ["orphan"] * w.cdc_orphan_creates_per_day
                       + [f"create:{c}" for c in crm_new.get(day, [])]
                       + [f"reinsert:{c}" for c in reinserts.get(day, [])])
            rng.shuffle(actions)
            stamps = sorted(tf.at(day) + timedelta(seconds=rng.randint(60, 86_000)) for _ in actions)
            for action, ts in zip(actions, stamps):
                live = sorted(state)
                if action.startswith(("create:", "reinsert:")):
                    cid = action.split(":", 1)[1]
                    if cid in state:
                        continue
                    state[cid] = self._profile(rng)
                    feats = ["S35:DELETE_REINSERT"] if action.startswith("reinsert") else []
                    ev = emit("c", cid, ts, None, state[cid], day, day, feats)
                    if chance(rng, 0.5):                     # created, then immediately completed
                        before = dict(state[cid])
                        state[cid]["account_manager"] = self._new_value(rng, "account_manager",
                                                                        before["account_manager"])
                        upd = emit("u", cid, ts + timedelta(seconds=1), before, state[cid], day, day)
                        ev.feats.append("S35:CREATE_THEN_UPDATE")
                        upd.feats.append("S35:CREATE_THEN_UPDATE")
                elif action == "orphan":
                    cid = f"C9{rng.randrange(100_000, 999_999)}"
                    state[cid] = self._profile(rng)
                    emit("c", cid, ts, None, state[cid], day, day, ["S35:ORPHAN_KEY"])
                elif action == "delete" and live:
                    cid = rng.choice(live)
                    before = state.pop(cid)
                    emit("d", cid, ts, before, None, day, day)
                    back = day + timedelta(days=rng.randint(2, 4))
                    if chance(rng, w.cdc_reinsert_share) and back <= self._anchor:
                        reinserts.setdefault(back, []).append(cid)
                elif live:
                    cid = rng.choice(live)
                    if action == "noop":
                        emit("u", cid, ts, state[cid], state[cid], day, day, ["S35:NOOP_UPDATE"])
                        continue
                    rounds = 2 if action == "same_ts" else 1
                    for _ in range(rounds):
                        before = dict(state[cid])
                        for fld in rng.sample(PROFILE_FIELDS, rng.choice((1, 1, 2))):
                            state[cid][fld] = self._new_value(rng, fld, before[fld])
                        eff = day - timedelta(days=rng.randint(1, 5)) if action == "backdated" else day
                        feats = {"same_ts": ["S35:SAME_COMMIT_TS"], "backdated": ["S35:BACKDATED"]}.get(action, [])
                        emit("u", cid, ts, before, state[cid], eff, day, feats)
            day += timedelta(days=1)
        self._cdc_final = state
        return events

    def _plan_cdc(self) -> None:
        rng = stream(self._seed, "cdc", "delivery")
        w = self._w
        by_day: dict[date | None, list[_Event]] = {}
        for ev in self._events:
            by_day.setdefault(ev.day, []).append(ev)
        files: dict[date | None, list[tuple[_Event, bool]]] = {d: [] for d in by_day}
        days = sorted(d for d in by_day if d is not None)
        for d in days:
            last = d == days[-1]
            deferred_create, on_time = False, set()
            for ev in by_day[d]:
                late = not last and ev.seq not in on_time and chance(rng, w.cdc_late_event_share)
                if (not last and not deferred_create and "S35:CREATE_THEN_UPDATE" in ev.feats
                        and ev.body["op"] == "c"):
                    late, deferred_create = True, True                  # the update arrives first
                    ev.feats.append("S35:UPDATE_BEFORE_INSERT")
                    nxt = next(e for e in by_day[d] if e.seq == ev.seq + 1)
                    nxt.feats.append("S35:UPDATE_BEFORE_INSERT")
                    on_time.add(nxt.seq)
                if late:
                    ev.late = True
                    ev.feats.append("S35:LATE_EVENT")
                target = d + timedelta(days=1) if late else d
                files.setdefault(target, []).append((ev, False))
                if chance(rng, w.cdc_duplicate_event_share):
                    dup_target = target + timedelta(days=1) if (chance(rng, 0.5) and target < days[-1]) else target
                    files.setdefault(dup_target, []).append((ev, True))
        files[None] = [(ev, False) for ev in by_day.get(None, [])]
        malformed_day = days[len(days) // 2]
        replay_source = self._day(-1)                                    # delivered D-1 03:00 (batch B003)
        for d, items in files.items():
            items.sort(key=lambda x: (x[0].late, x[0].seq, x[1]))         # late events trail the file
            if d is not None and len(items) > 12:
                i = rng.randrange(0, len(items) - 6)
                block = items[i:i + 5]
                items[i:i + 5] = block[::-1]                              # local out-of-order commit sequence
                for ev, _ in block:
                    if "S35:OUT_OF_ORDER" not in ev.feats:
                        ev.feats.append("S35:OUT_OF_ORDER")
            delivery = tf.at(self._start) + timedelta(minutes=30) if d is None else tf.at(d + timedelta(days=1), 3)
            keep = delivery.date() == replay_source
            self._deliveries.append(_Delivery(delivery, "crm_cdc", self._cdc_writer(
                delivery, items, malformed=(d == malformed_day), keep=keep)))
        replay_at = tf.at(self._day(2), 4)
        self._deliveries.append(_Delivery(replay_at, "crm_cdc", lambda write: self._cdc_replay(replay_at, write)))

    def _cdc_writer(self, delivery: datetime, items: list[tuple[_Event, bool]], malformed: bool, keep: bool):
        def run(write: bool) -> None:
            path = self._layout.crm_cdc(delivery)
            writer = JsonlWriter(path, write, keep_bytes=keep)
            ledger = self._rec.new_file(self._layout.relative(path), "crm_cdc",
                                        ["S35"] + (["S14"] if malformed else []))
            for i, (ev, dup) in enumerate(items):
                line = encode_json(ev.body)
                writer.write_line(line)
                feats = sorted(set(ev.feats) | ({"S35:DUPLICATE_EVENT"} if dup else set()))
                ledger.add(RecordLabel("crm_cdc", ev.body["customer_id"], ev.seq,
                                       "EXACT_DUPLICATE" if dup else "VALID", ["S35"] + feats, [],
                                       DUPLICATE_NOOP if dup else ACCEPTED, detail=ev.body["op"]))
                if malformed and i == len(items) // 2:
                    writer.write_line(line[:max(10, len(line) // 2)].rstrip(b"\n") + b"\n")
                    ledger.add(RecordLabel("crm_cdc", None, 0, "MALFORMED", ["S14", "S35"], ["SYS-PARSE"],
                                           QUARANTINE_PARSE, detail="TRUNCATED_JSON"))
            stats = writer.close()
            self._rec.close_file(ledger, stats.rows)
            if keep:
                self._kept["cdc"] = (stats, list(ledger.labels) if ledger.labels else None, ledger.rel_path)
        return run

    def _cdc_replay(self, delivery: datetime, write: bool) -> None:
        if "cdc" not in self._kept:
            return
        stats, _, source = self._kept["cdc"]
        path = self._layout.crm_cdc(delivery)
        copy = write_bytes(path, stats.content, stats.rows, write)
        ledger = self._rec.new_file(self._layout.relative(path), "crm_cdc", ["S19", "S35"])
        for _ in range(stats.rows):
            ledger.add(RecordLabel("crm_cdc", None, 0, "EXACT_DUPLICATE", ["S19", "S35"], [], DUPLICATE_NOOP))
        self._rec.close_file(ledger, copy.rows, DUPLICATE, f"SAME_CONTENT_AS:{source}")

    def write_truth(self) -> None:
        """Answer keys: CDC final state and its SCD2 history (commit-time validity, by source_seq)."""
        if "crm_cdc" not in self._cfg.sources:
            return
        out = ExpectedLayout(self._cfg.expected_root, self._cfg.profile.name).warehouse
        out.mkdir(parents=True, exist_ok=True)
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow(("customer_id",) + PROFILE_FIELDS)
        for cid in sorted(self._cdc_final):
            p = self._cdc_final[cid]
            w.writerow([cid] + ["" if p[f] is None else p[f] for f in PROFILE_FIELDS])
        (out / "cdc_customer_profile_final.csv").write_text(buf.getvalue(), encoding="ascii")
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow(("customer_id",) + PROFILE_FIELDS + ("valid_from_ts", "valid_to_ts", "is_current", "source_seq"))
        open_rows: dict[str, list] = {}
        rows: list[list] = []
        for ev in sorted(self._events, key=lambda e: e.seq):
            b = ev.body
            cid, ts = b["customer_id"], b["commit_ts"]
            if b["op"] == "u" and not b["changed_fields"]:
                continue                                                 # no-op: no new version
            if cid in open_rows:
                open_rows.pop(cid)[-3] = ts
            if b["op"] != "d":
                a = b["after"]
                row = [cid] + ["" if a[f] is None else a[f] for f in PROFILE_FIELDS] + [ts, "", "", b["source_seq"]]
                rows.append(row)
                open_rows[cid] = row
        for row in rows:
            row[-2] = "Y" if row[-3] == "" else "N"
        for row in sorted(rows, key=lambda r: (r[0], r[-1])):
            w.writerow(row)
        (out / "cdc_customer_profile_scd2.csv").write_text(buf.getvalue(), encoding="ascii")

    # ================================================================== S36 product catalog
    def _product_rows(self, version: int) -> list[tuple[list[str], list[str]]]:
        rows = []
        retired_on = self._day(-2)
        for code in sorted(PRODUCTS):
            fam = PRODUCT_FAMILY[code]
            feats: list[str] = []
            if version == 2 and code == "ALERTING":
                fam = "PLATFORM"
                feats.append("S36:REPARENTED")
            fam_name, unit_name = FAMILIES[fam]
            if version == 2 and fam == "DATA":
                fam_name = "Analytics & Insights"
                feats.append("S36:FAMILY_RENAMED")
            launch = date(2018, 1, 1) + timedelta(days=int(unit(self._seed, "product", code) * 2400))
            retired = ""
            active = "Y"
            if version == 2 and code == "CALENDAR-SYNC":
                retired, active = retired_on.isoformat(), "N"
                feats.append("S36:RETIRED")
            rows.append(([code, code.replace("-", " ").title(), fam, fam_name, unit_name, launch.isoformat(),
                          retired, active], feats))
        if version == 2:
            rows.append((["AI-COPILOT", "Ai Copilot", "PLATFORM", FAMILIES["PLATFORM"][0], FAMILIES["PLATFORM"][1],
                          self._day(-6).isoformat(), "", "Y"], ["S36:LATE_ADDED_MEMBER"]))
            rows.append(([" payments ", "Payments", "BILLING", FAMILIES["BILLING"][0], FAMILIES["BILLING"][1],
                          "2019-04-01", "", "Y"], ["S36:DIRTY_DUPLICATE_KEY"]))
        assert all(u not in PRODUCT_FAMILY for u in UNKNOWN_PRODUCTS)
        return rows

    def _plan_products(self) -> None:
        for snap_date, delivery, version in ((self._start, tf.at(self._start, 2, 30), 1),
                                             (self._day(-3), tf.at(self._day(-2), 2, 30), 1),
                                             (self._day(-1), tf.at(self._day(0), 2, 30), 2)):
            self._deliveries.append(_Delivery(delivery, "products", self._csv_writer(
                self._layout.products(snap_date, delivery), "products", PRODUCT_COLUMNS,
                lambda v=version: self._product_rows(v), ["S36"])))

    # ================================================================== S36 agent roster
    def _agent_history(self, num: int) -> tuple[list[list[str]], list[str], bool]:
        """Full effective-dated history of one agent: rows [team, site, role, from, to] and features."""
        w = self._w
        rng = stream(self._seed, "agents", num)
        teams = HD_TEAMS if num <= HELPDESK_AGENTS else CC_TEAMS
        team, site = rng.choice(teams), rng.choice(SITES)
        role = "TEAM_LEAD" if chance(rng, 0.05) else ("SENIOR_AGENT" if chance(rng, 0.25) else "AGENT")
        hired = self._start - timedelta(days=60 + int(unit(self._seed, "agents", "hire", num) * 1500))
        t = self._start + timedelta(days=rng.randint(1, max(1, (self._anchor - self._start).days)))
        other = rng.choice([x for x in teams if x != team])
        new_site = rng.choice(SITES) if chance(rng, 0.3) else site
        u = unit(self._seed, "agents", "case", num)
        cuts = [w.agent_transfer_share, w.agent_double_transfer_share, w.agent_overlap_share, w.agent_gap_share,
                w.agent_two_open_share, w.agent_leaver_share, w.agent_rehire_share]
        bounds, total = [], 0.0
        for c in cuts:
            total += c
            bounds.append(total)
        kind = next((name for name, b in zip(("TRANSFER", "DOUBLE", "OVERLAP", "GAP", "TWO_OPEN", "LEAVER",
                                               "REHIRE"), bounds) if u < b), None)
        iso = lambda d: d.isoformat()                                          # noqa: E731
        first = [team, site, role, iso(hired)]
        if kind is None:
            return [first + [""]], [], False
        if kind == "TRANSFER":
            return [first + [iso(t - timedelta(days=1))], [other, new_site, role, iso(t), ""]], [], False
        if kind == "DOUBLE":
            third = rng.choice([x for x in teams if x != other])
            return [first + [iso(t - timedelta(days=1))], [other, new_site, role, iso(t), iso(t)],
                    [third, new_site, role, iso(t + timedelta(days=1)), ""]], ["S36:ONE_DAY_ROW"], False
        if kind == "OVERLAP":
            corrected = unit(self._seed, "agents", "fix", num) < 0.5
            return [first + [iso(t + timedelta(days=1))], [other, new_site, role, iso(t), ""]], \
                ["S36:OVERLAP"] + (["S36:CORRECTED_IN_LATER_EXTRACT"] if corrected else []), corrected
        if kind == "GAP":
            return [first + [iso(t - timedelta(days=4))], [other, new_site, role, iso(t), ""]], ["S36:GAP"], False
        if kind == "TWO_OPEN":
            return [first + [""], [other, new_site, role, iso(t), ""]], ["S36:TWO_OPEN_ROWS"], False
        if kind == "LEAVER":
            return [first + [iso(t)]], ["S36:LEAVER"], False
        return [first + [iso(t)], [team, site, role, iso(t + timedelta(days=5)), ""]], ["S36:REHIRE"], False

    def _agent_rows(self, as_of: date, final: bool) -> list[tuple[list[str], list[str]]]:
        rows = []
        for num in range(1, HELPDESK_AGENTS + CALLCENTER_AGENTS + 1):
            if num in MISSING_AGENTS:
                continue
            rng = stream(self._seed, "agents", "name", num)
            name = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
            history, feats, corrected = self._agent_history(num)
            if final and corrected:
                history[0][4] = (date.fromisoformat(history[1][3]) - timedelta(days=1)).isoformat()
            for team, site, role, start, end in history:
                if date.fromisoformat(start) > as_of:
                    continue
                if end and date.fromisoformat(end) > as_of:
                    end = ""                                          # still open as of the extract
                rows.append(([agent_id(num), name, team, site, role, start, end], feats))
        return rows

    def _plan_agents(self) -> None:
        for as_of, delivery, final in ((self._day(-4), tf.at(self._day(-3), 4), False),
                                       (self._day(0), tf.at(self._day(1), 4), True)):
            self._deliveries.append(_Delivery(delivery, "agents", self._csv_writer(
                self._layout.agents(as_of, delivery), "agents", AGENT_COLUMNS,
                lambda a=as_of, f=final: self._agent_rows(a, f), ["S36"])))

    # ================================================================== S37 billing key map
    def _billing_rows(self, as_of: date, final: bool) -> list[tuple[list[str], list[str]]]:
        w = self._w
        rows = []
        nums = list(range(1, self._customers.base_size + 1))
        for k in (1, 2):
            if self._customers.signup_of_added(k) <= as_of:
                nums += self._customers.added_in(k)
        for num in nums:
            u = lambda *p: unit(self._seed, "billing", num, *p)        # noqa: E731
            if u("none") < w.billing_no_account_share:
                continue
            count = 1 + (u("second") < w.billing_second_account_share) + (u("third") < w.billing_third_account_share)
            for j in range(1, count + 1):
                ba = f"BA{num:07d}{j}"
                v = lambda *p: unit(self._seed, "billing", ba, *p)       # noqa: E731
                cid, feats = customer_id(num), []
                r = v("case")
                c1 = w.billing_orphan_share
                c2 = c1 + w.billing_blank_customer_share
                c3 = c2 + w.billing_key_format_share
                if r < c1:
                    cid, feats = f"C9{int(v('orphan') * 900_000) + 100_000}", ["S37:ORPHAN_KEY"]
                elif r < c2:
                    cid, feats = "", ["S37:BLANK_KEY"]
                elif r < c3:
                    cid, feats = (cid.lower() if v("fmt") < 0.5 else f" {cid} "), ["S37:KEY_FORMAT"]
                since = self._start - timedelta(days=30 + int(v("since") * 1000))
                base = [ba, cid, "PRIMARY" if j == 1 else "SECONDARY", CURRENCIES[int(v("cur") * len(CURRENCIES))],
                        PAYMENT_TERMS[int(v("terms") * len(PAYMENT_TERMS))], since.isoformat(), ""]
                moved_on = self._day(-3) + timedelta(days=int(v("moved_day") * 2))
                if v("moved") < w.billing_moved_share and moved_on <= as_of and cid:
                    target = customer_id(1 + int(v("to") * self._customers.base_size))
                    old = list(base)
                    old[6] = (moved_on - timedelta(days=1)).isoformat()
                    new = list(base)
                    new[1], new[5] = target, moved_on.isoformat()
                    rows.append((old, feats + ["S37:MOVED_ACCOUNT"]))
                    rows.append((new, feats + ["S37:MOVED_ACCOUNT"]))
                    continue
                rows.append((base, feats))
                if v("dup") < w.billing_duplicate_row_share:
                    rows.append((list(base), feats + ["S37:DUPLICATE_ROW"]))
                if v("shared") < w.billing_shared_account_share and cid:
                    other = list(base)
                    other[1] = customer_id(1 + int(v("shared_to") * self._customers.base_size))
                    rows.append((other, feats + ["S37:SHARED_ACCOUNT"]))
        return rows

    def _plan_billing(self) -> None:
        for as_of, delivery, final in ((self._day(-4), tf.at(self._day(-3), 5), False),
                                       (self._day(-1), tf.at(self._day(0), 5), True)):
            self._deliveries.append(_Delivery(delivery, "billing", self._csv_writer(
                self._layout.billing(as_of, delivery), "billing", BILLING_COLUMNS,
                lambda a=as_of, f=final: self._billing_rows(a, f), ["S37"])))

    # ================================================================== shared CSV writer
    def _csv_writer(self, path, source: str, columns, rows_fn, scenarios: list[str]):
        def run(write: bool) -> None:
            writer = CsvWriter(path, columns, write)
            ledger = self._rec.new_file(self._layout.relative(path), source, scenarios)
            for values, feats in rows_fn():
                writer.write_row(values)
                key = values[0].strip() or None
                ledger.add(RecordLabel(source, key, 1, "VALID", sorted(set(scenarios) | set(feats)), [], ACCEPTED))
            self._rec.close_file(ledger, writer.close().rows)
        return run
