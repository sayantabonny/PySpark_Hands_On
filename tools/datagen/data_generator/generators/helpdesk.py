"""Helpdesk simulation: tickets are created day by day, each gets a full event timeline
(NEW -> OPEN -> PENDING -> RESOLVED -> CLOSED, sometimes re-opened), and the helpdesk exports every
4 hours the latest state of every ticket that changed in that window. A ticket therefore appears
in several exports (S16) - these snapshots are what the pipeline receives.

The simulation streams: after day d has been generated, every export delivered up to d+1 00:00
is complete, because tickets created later cannot change the past. Only snapshots of still-open
tickets wait in memory.
"""
from __future__ import annotations

import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterator

from ..config.settings import GeneratorConfig
from ..core.rng import Weighted, Zipf, chance, lognormal, stream
from ..core.timefmt import EXPORT_SLOT, at
from ..model.records import HelpdeskTicket, Snapshot
from ..model.reference import (AGENT_CATEGORY, CATEGORIES, HD_CHANNELS, HELPDESK_AGENTS, HOT_PRODUCT,
                               LOCALES, PRIORITY_MIX, PRODUCTS, TAGS, UNKNOWN_PRODUCTS, agent_id)
from .customers import CustomerBase
from .text import TextFactory

FIRST_RESPONSE_MIN = {"P1": 20, "P2": 60, "P3": 180, "P4": 480}       # median minutes
RESOLVE_MIN = {"P1": 180, "P2": 480, "P3": 1_800, "P4": 4_200}        # median minutes
WEEKDAY_FACTOR = (1.12, 1.12, 1.12, 1.12, 1.12, 0.70, 0.50)            # Mon..Sun
OFFSETS = (timedelta(hours=5, minutes=30), timedelta(hours=-4), timedelta(hours=1), timedelta(hours=9))
FIRST_TICKET_NUM = 10_000_001
CSAT_BY_SENTIMENT = {s: Weighted(list(zip((1, 2, 3, 4, 5), w))) for s, w in (
    ("NEGATIVE", (40, 30, 15, 10, 5)), ("NEUTRAL", (5, 10, 30, 35, 20)), ("POSITIVE", (2, 3, 10, 35, 50)))}


@dataclass(frozen=True)
class _Event:
    at: datetime
    status: str
    resolved_at: datetime | None
    reopen_count: int
    csat: int | None
    assignee: str | None


class ProductPicker:
    def __init__(self, cfg: GeneratorConfig):
        self._others = [p for p in PRODUCTS if p != HOT_PRODUCT]
        self._zipf = Zipf(len(self._others), cfg.skew.product_zipf_s)
        self._hot_share = cfg.skew.hot_product_share
        self._unknown_share = cfg.skew.unknown_product_share

    def pick(self, rng: random.Random) -> str:
        r = rng.random()
        if r < self._unknown_share:
            return rng.choice(UNKNOWN_PRODUCTS)                 # S10
        if r < self._unknown_share + self._hot_share:
            return HOT_PRODUCT                                  # S29
        return self._others[self._zipf.pick(rng) - 1]


class HelpdeskSimulator:
    def __init__(self, cfg: GeneratorConfig, customers: CustomerBase, text: TextFactory):
        self._cfg = cfg
        self._customers = customers
        self._text = text
        self._products = ProductPicker(cfg)
        self._channels = Weighted(HD_CHANNELS)
        self._priorities = Weighted(PRIORITY_MIX)
        self._start = cfg.anchor_date - timedelta(days=cfg.profile.days - 1)
        self._horizon = at(cfg.anchor_date, 20)                  # last export: D 20:00
        self._next_num = FIRST_TICKET_NUM

    def exports(self) -> Iterator[tuple[datetime, list[Snapshot]]]:
        """Yields (delivery time, snapshots changed in the 4 hours before it), in time order."""
        pending: dict[datetime, list[Snapshot]] = defaultdict(list)
        mega = self._mega_ticket()
        for snap in self._snapshots(mega, stream(self._cfg.seed, "helpdesk", "mega")):
            pending[self._delivery_of(snap.updated_at)].append(snap)
        day = self._start
        while day <= self._cfg.anchor_date:
            for ticket, rng in self._tickets_for_day(day):
                for snap in self._snapshots(ticket, rng):
                    pending[self._delivery_of(snap.updated_at)].append(snap)
            first = at(day) + EXPORT_SLOT if day == self._start else at(day)
            delivery = first
            while delivery <= min(at(day) + timedelta(hours=20), self._horizon):
                snaps = pending.pop(delivery, [])
                snaps.sort(key=lambda s: (s.updated_at, s.ticket.num))
                yield delivery, snaps
                delivery += EXPORT_SLOT
            day += timedelta(days=1)

    @staticmethod
    def _delivery_of(ts: datetime) -> datetime:
        """An event at 09:13 is exported by the 12:00 export (window 08:00-12:00)."""
        slot_start = ts.replace(minute=0, second=0, microsecond=0) - timedelta(hours=ts.hour % 4)
        return slot_start + EXPORT_SLOT

    # ------------------------------------------------------------------ tickets
    def _tickets_for_day(self, day: date) -> Iterator[tuple[HelpdeskTicket, random.Random]]:
        rng = stream(self._cfg.seed, "helpdesk", "day", day.isoformat())
        base = self._cfg.profile.helpdesk_tickets_per_day * WEEKDAY_FACTOR[day.weekday()]
        incident = day == self._cfg.anchor_date
        count = round(base * (self._cfg.skew.incident_multiplier if incident else 1.0))
        normal = round(base)
        created = sorted(self._created_time(rng, day) for _ in range(count))
        for ts in created:
            is_incident = incident and rng.random() >= normal / count    # the extra volume (S29)
            yield self._ticket(rng, ts, is_incident), rng

    def _created_time(self, rng: random.Random, day: date) -> datetime:
        if chance(rng, 0.7):                       # business hours of the main customer base (IST)
            seconds = rng.randint(3 * 3600, 15 * 3600 - 1)
        else:
            seconds = rng.randint(0, 24 * 3600 - 1)
        return at(day) + timedelta(seconds=seconds)

    def _ticket(self, rng: random.Random, created: datetime, incident: bool) -> HelpdeskTicket:
        num = self._next_num
        self._next_num += 1
        product = "LOGIN-SSO" if incident else self._products.pick(rng)
        priority = rng.choice(("P1", "P2")) if incident else self._priorities.pick(rng)
        channel = self._channels.pick(rng)
        text = self._text.helpdesk_text(rng, product, priority, channel, incident)
        agent_cat = AGENT_CATEGORY[text.category]
        if chance(rng, self._cfg.text.agent_category_noise):
            agent_cat = AGENT_CATEGORY[rng.choice(CATEGORIES)]
        locale = rng.choice(LOCALES[text.language])
        return HelpdeskTicket(
            num=num, ticket_id=f"HD-{num:08d}", created_at=created,
            customer_id=self._customers.pick_for_ticket(rng, "helpdesk", created), channel=channel,
            product=product, priority=priority, agent_category=agent_cat, tags=TAGS[text.category],
            locale=locale, email=f"user{num % 97_331}@example.com", text=text,
            ts_offset=rng.choice(OFFSETS) if chance(rng, 0.15) else None, incident=incident)

    def _mega_ticket(self) -> HelpdeskTicket:
        rng = stream(self._cfg.seed, "helpdesk", "mega")
        created = at(self._start, 1)
        ticket = self._ticket(rng, created, incident=False)
        ticket.mega = True
        return ticket

    # ------------------------------------------------------------------ timelines
    def _events(self, t: HelpdeskTicket, rng: random.Random) -> list[_Event]:
        if t.mega:
            return self._mega_events(t)
        p = t.priority
        created = t.created_at
        events = [_Event(created, "NEW", None, 0, None, None)]
        agent = agent_id(1 + rng.randrange(HELPDESK_AGENTS))
        t_open = created + timedelta(minutes=max(1.0, lognormal(rng, FIRST_RESPONSE_MIN[p], 0.8)))
        events.append(_Event(t_open, "OPEN", None, 0, None, agent))
        t_resolve = created + timedelta(minutes=max(10.0, lognormal(rng, RESOLVE_MIN[p], 1.0)))
        if t_resolve <= t_open:
            t_resolve = t_open + timedelta(minutes=15)
        if chance(rng, 0.35):                                      # waiting on the customer
            t_pending = t_open + (t_resolve - t_open) * 0.4
            events.append(_Event(t_pending, "PENDING", None, 0, None, agent))
            events.append(_Event(t_open + (t_resolve - t_open) * 0.7, "OPEN", None, 0, None, agent))
        if chance(rng, 0.06):                                      # never resolved
            return events
        csat = self._csat(rng, t.text.sentiment)
        events.append(_Event(t_resolve, "RESOLVED", t_resolve, 0, csat, agent))
        if chance(rng, 0.05):                                      # re-opened once
            t_reopen = t_resolve + timedelta(minutes=lognormal(rng, 1_440, 0.6))
            events.append(_Event(t_reopen, "OPEN", None, 1, csat, agent))
            t_resolve2 = t_reopen + timedelta(minutes=lognormal(rng, RESOLVE_MIN[p], 0.8))
            events.append(_Event(t_resolve2, "RESOLVED", t_resolve2, 1, csat, agent))
            t_resolve = t_resolve2
        if chance(rng, 0.7):
            reopen = events[-1].reopen_count
            events.append(_Event(t_resolve + timedelta(days=3), "CLOSED", events[-1].resolved_at,
                                 reopen, csat, agent))
        return events

    def _mega_events(self, t: HelpdeskTicket) -> list[_Event]:
        """A long-running incident ticket updated in (almost) every export (S29 / S16)."""
        span = self._horizon - t.created_at
        count = self._cfg.skew.mega_ticket_versions
        step = max(EXPORT_SLOT, span / count)
        events, ts, i = [], t.created_at, 0
        while ts < self._horizon and len(events) < count:
            status = "NEW" if i == 0 else ("OPEN", "PENDING")[i % 2]
            events.append(_Event(ts, status, None, 0, None, agent_id(1) if i else None))
            ts += step
            i += 1
        return events

    @staticmethod
    def _csat(rng: random.Random, sentiment: str) -> int | None:
        return CSAT_BY_SENTIMENT[sentiment].pick(rng) if chance(rng, 0.4) else None

    def _snapshots(self, t: HelpdeskTicket, rng: random.Random) -> list[Snapshot]:
        """Latest state per export window; events after the horizon are never exported."""
        by_delivery: dict[datetime, _Event] = {}
        events = self._events(t, rng)
        t.events = tuple((ev.at, ev.status, ev.assignee, ev.reopen_count) for ev in events)
        for ev in events:
            if ev.at < self._horizon:
                by_delivery[self._delivery_of(ev.at)] = ev
        pending_label = "on-hold" if chance(rng, 0.1) else "pending"
        snaps = []
        for version, delivery in enumerate(sorted(by_delivery), start=1):
            ev = by_delivery[delivery]
            snaps.append(Snapshot(ticket=t, version_no=version, updated_at=ev.at, status=ev.status,
                                  resolved_at=ev.resolved_at, reopen_count=ev.reopen_count, csat=ev.csat,
                                  assignee=ev.assignee, pending_label=pending_label))
        return snaps
