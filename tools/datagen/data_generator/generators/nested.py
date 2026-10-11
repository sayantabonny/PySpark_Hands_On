"""Complex-type content for practising arrays, structs, maps and nested JSON (scenario S34).

Helpdesk records gain:
    comments       ARRAY<STRUCT<comment_id, author STRUCT<type, id>, created_at, public, body,
                                attachments ARRAY<STRUCT<file_name, content_type, size_bytes>>>>
    sla            STRUCT<policy, targets ARRAY<STRUCT<metric, target_minutes, breached>>,
                          pause_windows ARRAY<ARRAY<STRING>>>          (absent for the social channel)
    custom_fields  JSON object with a variable key set (read it as MAP<STRING, STRING>)

Comments follow the ticket's event timeline, so a later snapshot carries every earlier comment plus
the new ones (capped at the most recent `max_comments`). Agent comments have no `attachments` key,
customer comments an array (possibly empty) and system comments an explicit null - three different
"no attachments" shapes on purpose.

Call-centre rows gain IVR_JSON: a JSON object serialised into one CSV column.

Everything is derived from dedicated random streams keyed by the ticket number / call id, so the
same version of a ticket renders identically wherever it is delivered (late files, re-deliveries).
"""
from __future__ import annotations

import json
import random
from datetime import datetime, timedelta

from ..config.settings import NestedConfig
from ..core import timefmt as tf
from ..core.rng import chance, stream
from ..model import text_bank as tb
from ..model.records import HelpdeskTicket, Snapshot

SLA_POLICY = {"P1": "P1-CRITICAL", "P2": "P2-HIGH", "P3": "P3-STANDARD", "P4": "P4-LOW"}
FIRST_RESPONSE_TARGET = {"P1": 30, "P2": 90, "P3": 240, "P4": 480}           # minutes
RESOLUTION_TARGET = {"P1": 240, "P2": 720, "P3": 2_880, "P4": 5_760}           # minutes
NO_SLA_CHANNELS = ("social",)
_EXT = {"image/png": "png", "text/plain": "log", "application/json": "har", "application/pdf": "pdf",
        "text/csv": "csv"}


class NestedBuilder:
    def __init__(self, cfg: NestedConfig, seed: int):
        self._cfg, self._seed = cfg, seed

    # ================================================================== helpdesk
    def helpdesk_fields(self, snap: Snapshot) -> dict:
        """comments, sla and custom_fields of one snapshot, including any S34 damage."""
        t = snap.ticket
        timeline = self._timeline(t)
        comments = [c for at, c in timeline if at <= snap.updated_at][-self._cfg.max_comments:]
        fields = {"comments": comments, "sla": self._sla(t, snap), "custom_fields": self._custom_fields(t)}
        if t.channel in NO_SLA_CHANNELS:
            fields.pop("sla")
        self._damage(stream(self._seed, "helpdesk", "nested", t.num, snap.version_no), fields)
        return fields

    def damage_tags(self, rng_key: tuple, tags: list) -> list | None:
        """Tags are rendered by the feed; this applies the S34 tag edge cases to them."""
        rng = stream(self._seed, "helpdesk", "tags", *rng_key)
        cfg = self._cfg
        if chance(rng, cfg.null_arrays_share / 2):
            return None
        if not chance(rng, cfg.dirty_tags_share):
            return tags
        tags = list(tags) or ["general"]
        kind = rng.randrange(3)
        if kind == 0:
            tags.append(tags[0])                                   # duplicate element
        elif kind == 1:
            tags[0] = f" {tags[0].upper()} "                       # padded, upper-case element
        else:
            tags.insert(rng.randrange(len(tags) + 1), None)        # null element
        return tags

    def _timeline(self, t: HelpdeskTicket) -> list[tuple[datetime, dict]]:
        rng = stream(self._seed, "helpdesk", "comments", t.num)
        fmt = lambda dt: tf.iso(dt, t.ts_offset)                   # noqa: E731
        customer = {"type": "customer", "id": t.customer_id}
        out: list[tuple[datetime, dict]] = []

        def add(at: datetime, author: dict | None, public: bool, body: str, attachments="omit") -> None:
            c = {"comment_id": f"{t.ticket_id}-C{len(out) + 1:03d}", "author": author,
                 "created_at": fmt(at), "public": public, "body": body}
            if attachments != "omit":
                c["attachments"] = attachments
            out.append((at, c))

        prev, opened, reopen = None, False, 0
        for at, status, assignee, reopen_count in t.events:
            agent = {"type": "agent", "id": assignee}
            if status == "NEW" and t.channel != "api":           # API-created tickets start without one
                add(at, dict(customer), True, rng.choice(tb.CUSTOMER_FIRST), self._attachments(rng, t))
            elif status == "OPEN" and not opened:
                opened = True
                add(at, agent, True, rng.choice(tb.AGENT_FIRST))
                if chance(rng, self._cfg.internal_note_share):
                    add(at + timedelta(minutes=5), dict(agent), False, rng.choice(tb.INTERNAL_NOTES))
            elif status == "OPEN" and reopen_count > reopen:
                add(at, dict(customer), True, rng.choice(tb.CUSTOMER_REOPEN), self._attachments(rng, t))
            elif status == "OPEN" and prev == "PENDING":
                add(at, dict(customer), True, rng.choice(tb.CUSTOMER_REPLY), self._attachments(rng, t, 0.6))
            elif status == "OPEN":
                add(at, agent, True, rng.choice(tb.AGENT_UPDATE))
            elif status == "PENDING":
                add(at, agent, True, rng.choice(tb.AGENT_ASK_INFO))
            elif status == "RESOLVED":
                add(at, agent, True, rng.choice(tb.AGENT_RESOLVED))
            elif status == "CLOSED":
                add(at, {"type": "system", "id": None}, False, "Ticket closed automatically after 3 days.",
                    None)
            prev, reopen = status, reopen_count
        return out

    def _attachments(self, rng: random.Random, t: HelpdeskTicket, share: float | None = None) -> list[dict]:
        if not chance(rng, self._cfg.attachment_share if share is None else share):
            return []
        files = []
        for k in range(1, rng.randint(1, 3) + 1):
            stem, ctype, lo, hi = rng.choice(tb.ATTACHMENTS)
            files.append({"file_name": f"{stem}_{t.num % 1000:03d}_{k}.{_EXT[ctype]}",
                          "content_type": ctype, "size_bytes": rng.randint(lo, hi)})
        return files

    @staticmethod
    def _sla(t: HelpdeskTicket, snap: Snapshot) -> dict:
        fmt = lambda dt: tf.iso(dt, t.ts_offset)                   # noqa: E731
        seen = [e for e in t.events if e[0] <= snap.updated_at]
        first_open = next((e[0] for e in seen if e[1] == "OPEN"), None)
        resolved = next((e[0] for e in seen if e[1] == "RESOLVED"), None)
        windows, start = [], None
        for at, status, _, _ in seen:
            if status == "PENDING" and start is None:
                start = at
            elif status != "PENDING" and start is not None:
                windows.append([fmt(start), fmt(at)])
                start = None
        if start is not None:
            windows.append([fmt(start), None])                     # still paused at export time
        return {"policy": SLA_POLICY[t.priority],
                "targets": [_target("first_response", FIRST_RESPONSE_TARGET[t.priority], t.created_at,
                                    first_open, snap.updated_at),
                            _target("resolution", RESOLUTION_TARGET[t.priority], t.created_at, resolved,
                                    snap.updated_at)],
                "pause_windows": windows}

    def _custom_fields(self, t: HelpdeskTicket) -> dict:
        rng = stream(self._seed, "helpdesk", "custom_fields", t.num)
        keys = rng.sample(sorted(tb.CUSTOM_FIELD_VALUES), rng.randint(0, self._cfg.custom_fields_max))
        out = {}
        for key in sorted(keys):
            options = tb.CUSTOM_FIELD_VALUES[key]
            if options:
                out[key] = rng.choice(options)
            elif key == "erp_ref":
                out[key] = f"ERP-{rng.randrange(100_000, 999_999)}"
            else:
                out[key] = f"CTR-2026-{rng.randrange(1_000, 9_999)}"
        return out

    def _damage(self, rng: random.Random, f: dict) -> None:
        cfg, comments = self._cfg, f["comments"]
        if chance(rng, cfg.null_arrays_share / 2):
            f["comments"] = None
            return
        if comments and chance(rng, cfg.duplicate_comment_share):
            i = rng.randrange(len(comments))
            comments.insert(i + 1, json.loads(json.dumps(comments[i])))
        if len(comments) >= 2 and chance(rng, cfg.unordered_comments_share):
            if chance(rng, 0.5):
                comments.reverse()
            else:
                rng.shuffle(comments)
        with_files = [a for c in comments for a in (c.get("attachments") or [])]
        if with_files and chance(rng, cfg.size_as_text_share):
            a = rng.choice(with_files)
            a["size_bytes"] = rng.choice((str(a["size_bytes"]), f"{max(1, a['size_bytes'] // 1024)} KB"))
        if comments and chance(rng, cfg.null_author_share):
            rng.choice(comments)["author"] = None
        cf = f["custom_fields"]
        if cf and chance(rng, cfg.custom_field_drift_share):
            key = rng.choice(sorted(cf))
            if chance(rng, 0.5):
                cf[key.capitalize()] = cf.pop(key)                 # key case drift: 'Plan' vs 'plan'
            else:
                cf[key] = None                                     # null map value

    # ================================================================== call centre
    def ivr_json(self, call_id: str, disposition: str, queue: str) -> str:
        """IVR journey as JSON text inside a CSV column (empty for direct dial, rarely not JSON)."""
        rng = stream(self._seed, "callcenter", "ivr", call_id)
        if chance(rng, 0.05):
            return ""
        path = ["main"] + rng.sample(tb.IVR_MENUS[1:-1], rng.randint(0, 2))
        if disposition != "ABANDONED":
            path.append("agent")
        transfers = []
        if disposition == "ESCALATED":
            transfers = [{"queue": queue, "secs": rng.randint(20, 400)},
                         {"queue": "L2", "secs": rng.randint(60, 900)}]
        elif chance(rng, 0.1):
            transfers = [{"queue": queue, "secs": rng.randint(20, 300)}]
        ivr = {"entry": "main", "menu_path": path, "wait_secs": [rng.randint(3, 240) for _ in path],
               "callback_offered": disposition in ("CALLBACK", "ABANDONED") or chance(rng, 0.05),
               "transfers": transfers}
        if chance(rng, self._cfg.ivr_bad_json_share):
            return repr(ivr)                                       # legacy export: Python-style quotes
        return json.dumps(ivr, separators=(",", ":"))


def _target(metric: str, minutes: int, created: datetime, done: datetime | None, as_of: datetime) -> dict:
    due = created + timedelta(minutes=minutes)
    if done is not None:
        breached = done > due
    else:
        breached = True if as_of > due else None                   # unknown until met or overdue
    return {"metric": metric, "target_minutes": minutes, "breached": breached}


def helpdesk_features(rec: dict) -> list[str]:
    """S34 tags for a rendered helpdesk record, derived from its content (labels never guess)."""
    out: list[str] = []
    if "comments" in rec and rec["comments"] is None:
        out.append("S34:NULL_COMMENTS")
    if "tags" in rec and rec["tags"] is None:
        out.append("S34:NULL_TAGS")
    tags = rec.get("tags") or []
    if any(t is None for t in tags) or len(set(tags)) < len(tags) or any(
            isinstance(t, str) and t != t.strip().lower() for t in tags):
        out.append("S34:DIRTY_TAGS")
    comments = rec.get("comments") or []
    if not comments and rec.get("comments") is not None:
        out.append("S34:EMPTY_COMMENTS")
    ids = [c["comment_id"] for c in comments]
    if len(set(ids)) < len(ids):
        out.append("S34:DUPLICATE_COMMENT")
    if ids != sorted(ids):
        out.append("S34:UNORDERED_COMMENTS")
    if any(c["author"] is None for c in comments):
        out.append("S34:NULL_AUTHOR")
    if any(isinstance(a.get("size_bytes"), str) for c in comments for a in (c.get("attachments") or [])):
        out.append("S34:SIZE_AS_TEXT")
    cf = rec.get("custom_fields")
    if cf and (any(k != k.lower() for k in cf) or any(v is None for v in cf.values())):
        out.append("S34:CUSTOM_FIELD_DRIFT")
    if "sla" not in rec:
        out.append("S34:NO_SLA")
    return out


def ivr_features(value: str) -> list[str]:
    if not value:
        return ["S34:IVR_EMPTY"]
    try:
        json.loads(value)
    except json.JSONDecodeError:
        return ["S34:IVR_BAD_JSON"]
    return []
