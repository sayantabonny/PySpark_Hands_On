"""The CRM customer base and how it evolves between daily snapshots.

Customer attributes are derived from hashes of (seed, customer number, attribute), so the base is
never held in memory and any snapshot can be regenerated independently. Snapshot k contains the
original base plus customers added in snapshots 1..k, minus customers removed so far.
"""
from __future__ import annotations

import random
from datetime import timedelta
from typing import Iterator

from ..config.settings import GeneratorConfig
from ..core.rng import Zipf, chance, unit
from ..model.records import Customer
from ..model.reference import (INDUSTRIES, LEGAL_SUFFIX, PLACEHOLDER_CUSTOMER, REGIONS, TIERS,
                               customer_id)

_SYLLABLES = ("ka", "ven", "tor", "li", "sa", "mar", "den", "ro", "vi", "nex", "ta", "lo", "zen",
              "qua", "ri", "mo", "sol", "ar", "bel", "cor")
_SPECIAL_NAMES = (           # CSV-hostile but plain ASCII: commas, quotes, ampersands, apostrophes
    "Sharma, Rao & Associates", 'The "Blue Tiger" Company', "Mueller & Soehne GmbH", "Nandu Logistica S.A.",
    "Omega Software A.E.", "Severny Veter OOO", "O'Neil, Byrne & Co.", "Cafe Creme, Paris",
    "Lodz Data Sp. z o.o.", "Shri Ganesh Traders",
)
NEW_PER_SNAPSHOT_SHARE = 0.002
REMOVE_SHARE = 0.0005
TIER_CHANGE_SHARE = 0.002
INACTIVE_SHARE = 0.01


class CustomerBase:
    def __init__(self, cfg: GeneratorConfig):
        self._seed = cfg.seed
        self._n = cfg.profile.customers
        self._hot = cfg.skew.hot_customers
        self._skew = cfg.skew
        self._tail = Zipf(self._n - self._hot, cfg.skew.customer_zipf_s)
        self._tiers = [t for t, _ in TIERS]
        self._tier_cum = _cumulative(TIERS)
        self._region_cum = _cumulative(REGIONS)
        self._anchor = cfg.anchor_date
        self._early_share = cfg.warehouse.early_fact_share

    # ---------------------------------------------------------------- ticket side
    @property
    def base_size(self) -> int:
        return self._n

    @property
    def new_per_snapshot(self) -> int:
        return max(1, round(self._n * NEW_PER_SNAPSHOT_SHARE))

    def added_in(self, snapshot_no: int) -> list[int]:
        """Customer numbers first delivered in CRM snapshot `snapshot_no` (>= 1)."""
        first = self._n + (snapshot_no - 1) * self.new_per_snapshot + 1
        return list(range(first, first + self.new_per_snapshot))

    def signup_of_added(self, snapshot_no: int):
        return self._anchor - timedelta(days=5 - min(snapshot_no, 4))

    def is_early(self, cid: str | None) -> bool:
        """True for an id that only appears in a later CRM snapshot (early-arriving fact, S37)."""
        return bool(cid) and cid[1:].isdigit() and int(cid[1:]) > self._n

    def pick_for_ticket(self, rng: random.Random, source: str, when=None) -> str | None:
        """Helpdesk guests get the placeholder id; unidentified callers have no id at all. In the last
        days some facts name a customer that CRM delivers only later (S37 early-arriving fact)."""
        if when is not None and self._early_share and chance(rng, self._early_share):
            later = [k for k in (1, 2, 3) if self._anchor - timedelta(days=4 - k) > when.date()]
            if later and when.date() >= self._anchor - timedelta(days=5):
                return customer_id(rng.choice(self.added_in(rng.choice(later))))
        r = rng.random()
        if source == "helpdesk" and r < self._skew.placeholder_customer_share:
            return PLACEHOLDER_CUSTOMER
        if source == "callcenter" and r < self._skew.null_customer_share:
            return None
        if rng.random() < self._skew.hot_customer_share:
            return customer_id(1 + rng.randrange(self._hot))
        return customer_id(self._hot + self._tail.pick(rng))

    # ---------------------------------------------------------------- CRM side
    def snapshot(self, snapshot_no: int) -> Iterator[Customer]:
        new_per = max(1, round(self._n * NEW_PER_SNAPSHOT_SHARE))
        last = self._n + new_per * snapshot_no
        for num in range(1, last + 1):
            added_in = 0 if num <= self._n else 1 + (num - self._n - 1) // new_per
            customer = self._customer(num, added_in, snapshot_no)
            if customer is not None:
                yield customer

    def _customer(self, num: int, added_in: int, snapshot_no: int) -> Customer | None:
        hot = num <= self._hot
        if not hot and any(unit(self._seed, "cust", num, "remove", k) < REMOVE_SHARE
                           for k in range(max(1, added_in + 1), snapshot_no + 1)):
            return None
        u = lambda attr: unit(self._seed, "cust", num, attr)  # noqa: E731
        tier = "ENTERPRISE" if hot else _pick(self._tiers, self._tier_cum, u("tier"))
        for k in range(max(1, added_in + 1), snapshot_no + 1):          # tier changes over time
            if not hot and unit(self._seed, "cust", num, "tierchg", k) < TIER_CHANGE_SHARE:
                tier = _pick(self._tiers, self._tier_cum, unit(self._seed, "cust", num, "newtier", k))
        region = _pick([r for r, _ in REGIONS], self._region_cum, u("region"))
        signup = self._anchor - timedelta(days=30 + int(u("signup") * 3650))
        if added_in:
            signup = self._anchor - timedelta(days=5 - min(added_in, 4))
        return Customer(num=num, customer_id=customer_id(num), name=self._name(num, region, u),
                        tier=tier, region=region, industry=INDUSTRIES[int(u("industry") * len(INDUSTRIES))],
                        signup_date=signup, is_active=hot or u("inactive") >= INACTIVE_SHARE)

    def _name(self, num: int, region: str, u) -> str:
        if u("special") < 0.02:
            return _SPECIAL_NAMES[int(u("special_pick") * len(_SPECIAL_NAMES))]
        parts = [_SYLLABLES[int(unit(self._seed, "cust", num, "syl", i) * len(_SYLLABLES))]
                 for i in range(2 + int(u("len") * 2))]
        suffixes = LEGAL_SUFFIX[region]
        return f"{''.join(parts).capitalize()} {suffixes[int(u('suffix') * len(suffixes))]}"


def _cumulative(pairs) -> list[float]:
    total, out = 0.0, []
    for _, w in pairs:
        total += w
        out.append(total)
    return [c / total for c in out]


def _pick(items, cum: list[float], u: float):
    for item, c in zip(items, cum):
        if u < c:
            return item
    return items[-1]

