"""The delivery timeline (PDF 1, Section 22.8): which deliveries belong to which batch and where each
file-level scenario is placed. D is the anchor date.

    B001  backfill      helpdesk exports up to D-3 20:00, call-centre days up to D-4, CRM snapshot D-4
    B002  day D-2       normal day + one S21 file
    B003  day D-1       drift and duplicates: S26B S24 S27 S18 in exports, S17 late file, S19 copy,
                        call-centre S24+S27 file, S20 correction
    B004  day D         incident (S29) + rejections: S22 S23A S23B S25 S26 in exports, S28 re-export,
                        partial CRM snapshot (S32)
    B005  day D+1       corrected re-deliveries of the S22/S23/S25 files, full CRM snapshot
    B006  day D+2       replay: only a byte-identical copy of an already loaded file (S19)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from ..core.timefmt import at


@dataclass(frozen=True)
class CrmDelivery:
    snapshot_no: int
    snapshot_date: date
    delivery: datetime
    partial: bool = False


@dataclass(frozen=True)
class BatchSpec:
    batch_id: str
    purpose: str
    helpdesk_until: datetime | None               # emit regular exports delivered up to this time
    helpdesk_slot_scenarios: dict[int, str] = field(default_factory=dict)   # export hour -> scenario
    s21_mode: str = "none"                         # none | every_nth | one
    callcenter_days: tuple[date, ...] = ()
    callcenter_day_scenarios: tuple[str, ...] = ()   # applied to the batch's call-centre day file
    crm: CrmDelivery | None = None
    extras: tuple[tuple[str, datetime], ...] = ()  # extra deliveries: (kind, delivery time)
    recommended_as_of: datetime | None = None
    expected_events: tuple[str, ...] = ()


def build_plan(anchor: date, days: int) -> list[BatchSpec]:
    d = anchor
    first = d - timedelta(days=days - 1)
    day = lambda n: d + timedelta(days=n)                  # noqa: E731
    cc_backfill = tuple(first + timedelta(days=i) for i in range((day(-4) - first).days + 1))
    return [
        BatchSpec("B001", "Backfill: initial load at volume", helpdesk_until=at(day(-3), 20),
                  s21_mode="every_nth", callcenter_days=cc_backfill,
                  crm=CrmDelivery(0, day(-4), at(day(-3), 2)),
                  recommended_as_of=at(day(-3), 23),
                  expected_events=("S21: files LOADED with a few PARSE quarantines",)),
        BatchSpec("B002", "Normal day", helpdesk_until=at(day(-2), 20), s21_mode="one",
                  callcenter_days=(day(-3),), crm=CrmDelivery(1, day(-3), at(day(-2), 2)),
                  recommended_as_of=at(day(-2), 23)),
        BatchSpec("B003", "Drift tolerance, ordering and duplicates", helpdesk_until=at(day(-1), 20),
                  helpdesk_slot_scenarios={0: "S26B", 4: "S24", 8: "S27", 16: "S18"},
                  callcenter_days=(day(-2),), callcenter_day_scenarios=("S24", "S27"),
                  crm=CrmDelivery(2, day(-2), at(day(-1), 2)),
                  extras=(("S20_CORRECTION", at(day(-1), 12)), ("S17_LATE_FILE", at(day(-1), 12, 30)),
                          ("S19_COPY", at(day(-1), 21))),
                  recommended_as_of=at(day(-1), 23),
                  expected_events=("SCHEMA_DRIFT events for S24/S26B files", "SYS-DUP-CONFLICT for S18 pairs",
                                   "S19 copy -> DUPLICATE")),
        BatchSpec("B004", "Incident day and rejections", helpdesk_until=at(day(0), 20),
                  helpdesk_slot_scenarios={4: "S22", 8: "S23A", 12: "S23B", 16: "S25", 20: "S26"},
                  callcenter_days=(day(-1),), crm=CrmDelivery(3, day(-1), at(day(0), 2), partial=True),
                  extras=(("S28_BACKLOG", at(day(0), 13)),),
                  recommended_as_of=at(day(0), 23),
                  expected_events=("DEVIATION: daily_volume (incident, S29)",
                                   "Files REJECTED: S22 MALFORMED_RATIO, S23A CONTROL_MISMATCH, "
                                   "S23B CONTROL_FILE_MISSING, S25/S26 MISSING_REQUIRED_FIELD",
                                   "CRM REJECTED: PARTIAL_SNAPSHOT")),
        BatchSpec("B005", "Recovery: corrected re-deliveries", helpdesk_until=None,
                  crm=CrmDelivery(3, day(-1), at(day(1), 2)), extras=(("REDELIVERY", at(day(1), 6)),),
                  recommended_as_of=at(day(1), 8),
                  expected_events=("Corrected files LOADED; older versions inside them may be STALE",
                                   "S26: add alias createdAt -> created_at and REPROCESS the B004 file")),
        BatchSpec("B006", "Replay: idempotency", helpdesk_until=None,
                  extras=(("S19_COPY_B006", at(day(2), 6)),), recommended_as_of=at(day(2), 8),
                  expected_events=("Zero data changes; the copy is DUPLICATE",)),
    ]
