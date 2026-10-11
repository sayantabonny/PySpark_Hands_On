"""Domain objects produced by the generators. They describe the *truth* (what really happened);
rendering into source formats and deliberate damage happen later, in feeds and scenarios."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta


@dataclass(frozen=True)
class TicketText:
    subject: str | None
    description: str
    category: str               # intended (ground truth for LLM evaluation)
    sentiment: str
    urgency: int
    language: str
    features: tuple[str, ...]   # scenario tags contributed by the text, e.g. "S12:LONG_TEXT"
    fault: str | None = None    # FakeLLM fault marker kind (S31)


@dataclass(frozen=True)
class Customer:
    num: int
    customer_id: str
    name: str
    tier: str
    region: str
    industry: str
    signup_date: date
    is_active: bool


@dataclass
class HelpdeskTicket:
    num: int
    ticket_id: str
    created_at: datetime
    customer_id: str
    channel: str                # source value: email | chat | web | api | social
    product: str
    priority: str               # canonical P1..P4
    agent_category: str         # agent-selected, deliberately noisy
    tags: tuple[str, ...]
    locale: str
    email: str
    text: TicketText
    ts_offset: timedelta | None  # ticket-level timestamp style (None = 'Z')
    incident: bool = False
    mega: bool = False
    events: tuple = ()          # (at, status, assignee, reopen_count) timeline, set by the simulator


@dataclass(frozen=True)
class Snapshot:
    """The state of a ticket as exported in one 4-hour helpdesk export."""
    ticket: HelpdeskTicket
    version_no: int
    updated_at: datetime
    status: str                 # canonical
    resolved_at: datetime | None
    reopen_count: int
    csat: int | None
    assignee: str | None
    pending_label: str = "pending"   # source spelling of PENDING: pending | on-hold


@dataclass(frozen=True)
class Call:
    call_id: str
    business_date: date
    customer_id: str | None
    start: datetime
    end: datetime
    agent: str
    queue: str
    priority: str
    disposition: str
    handle_secs: int
    product: str
    csat: int | None
    text: TicketText
    ivr: str = ""               # IVR journey as JSON text (S34), rendered into the IVR_JSON column
