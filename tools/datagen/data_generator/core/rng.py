"""Deterministic randomness.

Every purpose gets its own stream derived from (seed, *parts). Adding a scenario, changing one
source or generating only some batches therefore never shifts the random numbers used elsewhere,
which is what makes outputs byte-identical and scenarios independently reproducible.
Guarantee: identical output for the same seed, configuration and Python minor version.
"""
from __future__ import annotations

import bisect
import hashlib
import math
import random
from typing import Generic, Sequence, TypeVar

T = TypeVar("T")


def _digest(seed: int, parts: tuple[object, ...]) -> bytes:
    key = "|".join([str(seed), *map(str, parts)]).encode("utf-8")
    return hashlib.sha256(key).digest()


def stream(seed: int, *parts: object) -> random.Random:
    """An independent, reproducible random stream for one purpose."""
    return random.Random(int.from_bytes(_digest(seed, parts)[:8], "big"))


def unit(seed: int, *parts: object) -> float:
    """A deterministic value in [0, 1) for per-entity attributes, without creating a Random."""
    return int.from_bytes(_digest(seed, parts)[:7], "big") / float(1 << 56)


class Weighted(Generic[T]):
    """Weighted choice with O(log n) sampling."""

    def __init__(self, pairs: Sequence[tuple[T, float]]):
        self._items: list[T] = []
        self._cum: list[float] = []
        total = 0.0
        for item, weight in pairs:
            if weight < 0:
                raise ValueError(f"negative weight for {item!r}")
            if weight == 0:
                continue
            total += weight
            self._items.append(item)
            self._cum.append(total)
        if not self._items:
            raise ValueError("at least one positive weight is required")
        self._total = total

    def pick(self, rng: random.Random) -> T:
        return self._items[bisect.bisect_right(self._cum, rng.random() * self._total)]


class Zipf:
    """Ranks 1..n with P(k) proportional to k**-s, sampled by inverse CDF."""

    def __init__(self, n: int, s: float):
        if n < 1:
            raise ValueError("n must be >= 1")
        cum: list[float] = []
        total = 0.0
        for k in range(1, n + 1):
            total += k ** -s
            cum.append(total)
        self._cum, self._total, self.n = cum, total, n

    def pick(self, rng: random.Random) -> int:
        return bisect.bisect_right(self._cum, rng.random() * self._total) + 1


def chance(rng: random.Random, p: float) -> bool:
    return rng.random() < p


def lognormal(rng: random.Random, median: float, sigma: float) -> float:
    """Log-normal sample parameterised by its median (easier to reason about than mu)."""
    return median * math.exp(sigma * rng.gauss(0.0, 1.0))
