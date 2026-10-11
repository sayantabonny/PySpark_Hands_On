"""Builds ticket and call-note text together with its ground truth (intended category, sentiment,
urgency) and the scenario features the text carries. All text is plain ASCII English."""
from __future__ import annotations

import random

from ..config.settings import TextConfig
from ..core.rng import Weighted, chance
from ..model import text_bank as tb
from ..model.records import TicketText
from ..model.reference import CATEGORIES, primary_category
from ..scenarios.catalog import CALL_LLM_PROBLEM_KINDS, FAULTS, LLM_PROBLEM_KINDS

_BASE_URGENCY = {"P1": 5, "P2": 4, "P3": 2, "P4": 1}
_SENTIMENT_BY_PRIORITY = {
    "P1": (("NEGATIVE", 60.0), ("NEUTRAL", 35.0), ("POSITIVE", 5.0)),
    "P2": (("NEGATIVE", 40.0), ("NEUTRAL", 52.0), ("POSITIVE", 8.0)),
    "P3": (("NEGATIVE", 25.0), ("NEUTRAL", 63.0), ("POSITIVE", 12.0)),
    "P4": (("NEGATIVE", 15.0), ("NEUTRAL", 65.0), ("POSITIVE", 20.0)),
}
_DATES = ("yesterday", "last Monday", "1 September", "this morning", "the weekend")


class TextFactory:
    def __init__(self, cfg: TextConfig):
        self._cfg = cfg
        self._sentiment = {p: Weighted(w) for p, w in _SENTIMENT_BY_PRIORITY.items()}
        self._problems = Weighted(LLM_PROBLEM_KINDS)
        self._call_problems = Weighted(CALL_LLM_PROBLEM_KINDS)
        self._faults = sorted(FAULTS)

    # ------------------------------------------------------------------ helpdesk
    def helpdesk_text(self, rng: random.Random, product: str, priority: str, channel: str,
                      incident: bool = False) -> TicketText:
        category = "ACCOUNT_ACCESS" if incident else self._category(rng, product)
        sentiment = "NEGATIVE" if incident else self._sentiment[priority].pick(rng)
        urgency = self._urgency(rng, priority)
        if incident:
            return TicketText("SSO login failing for all users", tb.INCIDENT_TEXT, category, sentiment,
                              5, "en", ("S29:INCIDENT", "S33:TEMPLATED"))
        features: list[str] = []
        if chance(rng, self._cfg.templated_share):
            description = rng.choice(tb.CANNED[category])
            subject = description.split(".")[0][:60]
            features.append("S33:TEMPLATED")
        else:
            slots = self._slots(rng, product)
            subject, description = self._compose_en(rng, category, sentiment, urgency, slots)
        subject, description, fault = self._extras(rng, subject, description, category, product,
                                                   channel, features)
        return TicketText(subject, description, category, sentiment, urgency, "en",
                          tuple(features), fault)

    def _compose_en(self, rng, category, sentiment, urgency, slots) -> tuple[str, str]:
        subject = rng.choice(tb.SUBJECTS[category]).format(**slots)
        parts = [rng.choice(tb.OPENERS[sentiment]), rng.choice(tb.PROBLEMS[category]).format(**slots)]
        if chance(rng, 0.3):
            parts.append(rng.choice(tb.PROBLEMS[category]).format(**slots))
        if urgency >= 4 and chance(rng, 0.5):
            parts.append(rng.choice(tb.URGENT_CUES))
        elif urgency <= 2 and chance(rng, 0.2):
            parts.append(rng.choice(tb.CALM_CUES))
        parts.append(rng.choice(tb.CLOSERS[sentiment]))
        joiner = rng.choice(("\n", " ", " ", "\r\n", "\n\n"))
        return subject, joiner.join(parts)

    def _extras(self, rng, subject, description, category, product, channel, features):
        cfg, fault = self._cfg, None
        if chance(rng, cfg.llm_problem_share):
            kind = self._problems.pick(rng)
            subject, description, fault = self._llm_problem(rng, kind, subject, description, category,
                                                            product)
            features.append(f"S31:{fault}" if fault else f"S30:{kind}")
            return subject, description, fault
        if channel in ("email", "web") and chance(rng, cfg.html_share):
            description = tb.HTML_TEMPLATE.format(a=subject, b=description)
            features.append("S12:HTML")
        if chance(rng, cfg.very_long_text_share):
            description = self._pad(rng, description, rng.randint(100_000, 200_000))
            features.append("S12:VERY_LONG_TEXT")
        elif chance(rng, cfg.long_text_share):
            description = self._pad(rng, description, rng.randint(20_001, 100_000))
            features.append("S12:LONG_TEXT")
        if chance(rng, cfg.empty_description_share):
            description = ""
            features.append("S12:EMPTY_DESCRIPTION")
        return subject, description, fault

    def _llm_problem(self, rng, kind, subject, description, category, product):
        if kind == "EMPTY_TEXT":
            return "", "", None
        if kind == "PROMPT_INJECTION":
            return subject, f"{description}\n{rng.choice(tb.INJECTIONS)}", None
        if kind == "PII_HEAVY":
            return subject, f"{description} {' '.join(rng.sample(tb.PII_TEXTS, 2))}", None
        if kind == "CODE_LOG_DUMP":
            return subject, f"{description}\n{rng.choice(tb.LOG_DUMPS)}", None
        if kind == "AMBIGUOUS":
            other = rng.choice([c for c in CATEGORIES if c != category])
            extra = rng.choice(tb.PROBLEMS[other]).format(**self._slots(rng, product))
            return subject, f"{description} Also, {extra}", None
        fault = rng.choice(self._faults)                      # FAULT_MARKER
        return subject, f"{description} {FAULTS[fault].marker}", fault

    # ------------------------------------------------------------------ call centre
    def call_text(self, rng: random.Random, product: str, priority: str, disposition: str) -> TicketText:
        category = self._category(rng, product)
        sentiment = {"ESCALATED": "NEGATIVE", "ABANDONED": "NEGATIVE"}.get(
            disposition, self._sentiment[priority].pick(rng))
        urgency = self._urgency(rng, priority)
        if disposition == "ABANDONED" and chance(rng, 0.8):
            return TicketText(None, "", category, sentiment, urgency, "en", ("S12:EMPTY_NOTES",))
        problem = rng.choice(tb.PHONE_PROBLEMS[category]).format(**self._slots(rng, product))
        sep = "\n" if chance(rng, 0.3) else " "
        notes = f"Caller reports {problem}.{sep}{rng.choice(tb.PHONE_ACTIONS)}"
        features: list[str] = []
        fault = None
        if chance(rng, self._cfg.llm_problem_share):
            kind = self._call_problems.pick(rng)
            if kind == "EMPTY_TEXT":
                notes = ""
            elif kind == "PII_HEAVY":
                notes = f"{notes} {rng.choice(tb.PII_TEXTS)}"
            elif kind == "CODE_LOG_DUMP":
                notes = f"{notes}\n{rng.choice(tb.LOG_DUMPS)}"
            else:
                fault = rng.choice(self._faults)
                notes = f"{notes} {FAULTS[fault].marker}"
            features.append(f"S31:{fault}" if fault else f"S30:{kind}")
        return TicketText(None, notes, category, sentiment, urgency, "en", tuple(features), fault)

    # ------------------------------------------------------------------ helpers
    def _category(self, rng, product: str) -> str:
        return primary_category(product) if chance(rng, 0.55) else rng.choice(CATEGORIES)

    @staticmethod
    def _urgency(rng, priority: str) -> int:
        noise = rng.choice((-1, 0, 0, 0, 1))
        return max(1, min(5, _BASE_URGENCY[priority] + noise))

    @staticmethod
    def _slots(rng, product: str) -> dict[str, str]:
        """One set of placeholder values per text, so the subject and the description of a
        ticket mention the same invoice, order and error code."""
        values = {key: rng.choice(options) for key, options in tb.SLOTS.items()}
        values.update(product=product, invoice=f"INV-{rng.randrange(100_000, 999_999)}",
                      order=f"ORD-{rng.randrange(1_000_000, 9_999_999)}", date=rng.choice(_DATES))
        return values

    @staticmethod
    def _pad(rng, description: str, target: int) -> str:
        """Pads with realistic pasted log output until the target length is reached."""
        chunks = [description, "\n--- pasted log ---\n"]
        size = sum(map(len, chunks))
        while size < target:
            line = rng.choice(tb.LOG_DUMPS) + "\n"
            chunks.append(line)
            size += len(line)
        return "".join(chunks)[:target]
