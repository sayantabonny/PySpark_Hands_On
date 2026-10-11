"""Call-centre simulation: one immutable row per call, grouped by business date (IST).
Call times have minute precision because the source format has no seconds."""
from __future__ import annotations

import random
from datetime import date, datetime, timedelta

from ..config.settings import GeneratorConfig
from ..core.rng import Weighted, chance, lognormal, stream
from ..core.timefmt import IST, UTC
from ..model.records import Call
from ..model.reference import (CALLCENTER_AGENTS, DISPOSITIONS, HELPDESK_AGENTS, PHONE_PRIORITY_MIX,
                               QUEUE, agent_id)
from .customers import CustomerBase
from .helpdesk import CSAT_BY_SENTIMENT, ProductPicker
from .nested import NestedBuilder
from .text import TextFactory

WEEKDAY_FACTOR = (1.1, 1.1, 1.1, 1.1, 1.1, 0.6, 0.4)
BACKLOG_SEQ_OFFSET = 900_000          # re-exported calls (S28) use their own id range


class CallcenterSimulator:
    def __init__(self, cfg: GeneratorConfig, customers: CustomerBase, text: TextFactory):
        self._cfg = cfg
        self._customers = customers
        self._text = text
        self._products = ProductPicker(cfg)
        self._priorities = Weighted(PHONE_PRIORITY_MIX)
        self._dispositions = Weighted(DISPOSITIONS)
        self._nested = NestedBuilder(cfg.nested, cfg.seed)

    def calls_for_day(self, business_date: date) -> list[Call]:
        rng = stream(self._cfg.seed, "callcenter", "day", business_date.isoformat())
        count = round(self._cfg.profile.callcenter_calls_per_day * WEEKDAY_FACTOR[business_date.weekday()])
        return self._calls(rng, business_date, count, seq_offset=0)

    def backlog_calls(self, business_date: date, count: int) -> list[Call]:
        rng = stream(self._cfg.seed, "callcenter", "backlog", business_date.isoformat())
        return self._calls(rng, business_date, count, seq_offset=BACKLOG_SEQ_OFFSET)

    def _calls(self, rng: random.Random, business_date: date, count: int, seq_offset: int) -> list[Call]:
        opening = datetime(business_date.year, business_date.month, business_date.day, 9, tzinfo=IST)
        starts = sorted(opening + timedelta(minutes=rng.randrange(12 * 60)) for _ in range(count))
        return [self._call(rng, business_date, i + 1 + seq_offset, start.astimezone(UTC))
                for i, start in enumerate(starts)]

    def _call(self, rng: random.Random, business_date: date, seq: int, start: datetime) -> Call:
        minutes = max(1, round(lognormal(rng, 6.0, 0.7)))
        disposition = self._dispositions.pick(rng)
        product = self._products.pick(rng)
        priority = self._priorities.pick(rng)
        text = self._text.call_text(rng, product, priority, disposition)
        csat = None
        if disposition == "RESOLVED" and chance(rng, 0.4):
            csat = CSAT_BY_SENTIMENT[text.sentiment].pick(rng)
        call_id = f"CC{business_date:%Y%m%d}{seq:06d}"
        return Call(
            call_id=call_id, business_date=business_date,
            customer_id=self._customers.pick_for_ticket(rng, "callcenter", start), start=start,
            end=start + timedelta(minutes=minutes),
            agent=agent_id(HELPDESK_AGENTS + 1 + rng.randrange(CALLCENTER_AGENTS)),
            queue=QUEUE[text.category], priority=priority, disposition=disposition,
            handle_secs=max(1, minutes * 60 - rng.randrange(60)), product=product, csat=csat, text=text,
            ivr=self._nested.ivr_json(call_id, disposition, QUEUE[text.category]))
