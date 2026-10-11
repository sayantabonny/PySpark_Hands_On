"""Volume profiles (PDF 1, Section 2.5). Ticket rates are calibrated so that the number of helpdesk
*snapshots* (a ticket re-appears in every export in which it changed) lands near the target."""
from __future__ import annotations

from .settings import VolumeProfile

PROFILES: dict[str, VolumeProfile] = {
    # target ~40k helpdesk snapshots, ~10k calls, 5k customers, 14 days
    "small": VolumeProfile(name="small", days=14, helpdesk_tickets_per_day=650,
                           callcenter_calls_per_day=800, customers=5_000, max_records_per_file=2_000),
    # target ~2.0M snapshots, ~400k calls, 150k customers, 90 days
    "medium": VolumeProfile(name="medium", days=90, helpdesk_tickets_per_day=5_800,
                            callcenter_calls_per_day=4_750, customers=150_000),
    # target ~10M snapshots, ~2M calls, 500k customers, 180 days
    "large": VolumeProfile(name="large", days=180, helpdesk_tickets_per_day=14_600,
                           callcenter_calls_per_day=11_800, customers=500_000,
                           record_labels=False, track_keys=False),
}
