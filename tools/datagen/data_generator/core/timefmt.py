"""Time helpers. All datetimes inside the generator are timezone-aware UTC; formatting to the
source-specific representations happens only here."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30), "IST")      # call centre, Bengaluru (no DST)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
EXPORT_SLOT = timedelta(hours=4)                              # helpdesk export cadence


def at(d: date, hour: int = 0, minute: int = 0, second: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hour, minute, second, tzinfo=UTC)


def iso(dt: datetime, offset: timedelta | None = None) -> str:
    """ISO-8601 with 'Z' or a numeric offset, e.g. 2026-08-01T14:45:22+05:30."""
    if offset is None:
        return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    local = dt.astimezone(timezone(offset))
    minutes = int(offset.total_seconds() // 60)
    sign = "+" if minutes >= 0 else "-"
    minutes = abs(minutes)
    return local.strftime("%Y-%m-%dT%H:%M:%S") + f"{sign}{minutes // 60:02d}:{minutes % 60:02d}"


def iso_millis(dt: datetime) -> str:
    u = dt.astimezone(UTC)
    return u.strftime("%Y-%m-%dT%H:%M:%S") + f".{u.microsecond // 1000:03d}Z"


def space_utc(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")


def epoch_ms(dt: datetime) -> str:
    return str((dt - EPOCH) // timedelta(milliseconds=1))


def ist_dmy(dt: datetime) -> str:
    """Call-centre contract format: dd/MM/yyyy HH:mm, local time without offset."""
    return dt.astimezone(IST).strftime("%d/%m/%Y %H:%M")


def ist_mdy(dt: datetime) -> str:
    """The wrong format used by the drifted re-export (S28)."""
    return dt.astimezone(IST).strftime("%m/%d/%Y %H:%M")


def compact(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def epoch_s(dt: datetime) -> int:
    return int((dt - EPOCH).total_seconds())
