"""Typed, immutable generator configuration.

Everything that changes the *content* of generated files lives here and is part of the
configuration fingerprint written to generation_manifest.json. Output locations and the
batch/source filters only decide what is *written*, so they are excluded from the fingerprint.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

ALL_BATCHES: tuple[str, ...] = ("B001", "B002", "B003", "B004", "B005", "B006")
ALL_SOURCES: tuple[str, ...] = ("crm", "callcenter", "helpdesk", "crm_cdc", "products", "agents", "billing")


@dataclass(frozen=True)
class VolumeProfile:
    name: str
    days: int                          # timeline length, ending on the anchor date (inclusive)
    helpdesk_tickets_per_day: int      # new tickets on an average day (snapshots emerge from timelines)
    callcenter_calls_per_day: int
    customers: int
    max_records_per_file: int = 20_000
    record_labels: bool = True         # write per-record ground truth (large profile: off, size)
    track_keys: bool = True            # key-level expectations (large profile: off, memory)


@dataclass(frozen=True)
class RecordMix:
    """Quality class of each emitted record, in percent. Mutually exclusive; must sum to 100."""
    valid: float = 86.0
    mild: float = 7.0
    nullify: float = 2.0
    dq_error: float = 2.5
    malformed: float = 0.5
    exact_duplicate: float = 2.0       # record is emitted normally AND an identical copy follows

    def pairs(self) -> list[tuple[str, float]]:
        return [("VALID", self.valid), ("MILD", self.mild), ("NULLIFY", self.nullify),
                ("DQ_ERROR", self.dq_error), ("MALFORMED", self.malformed),
                ("EXACT_DUPLICATE", self.exact_duplicate)]


@dataclass(frozen=True)
class SkewConfig:
    placeholder_customer_share: float = 0.06    # helpdesk guest tickets -> C0000000
    null_customer_share: float = 0.01           # call centre: unidentified caller
    hot_customers: int = 10
    hot_customer_share: float = 0.15
    customer_zipf_s: float = 0.8                 # long tail over the remaining customers
    hot_product_share: float = 0.25             # LOGIN-SSO
    product_zipf_s: float = 1.0
    unknown_product_share: float = 0.003
    incident_multiplier: float = 8.0            # ticket volume on the anchor day (S29)
    mega_ticket_versions: int = 300


@dataclass(frozen=True)
class TextConfig:
    """All generated text is English and plain ASCII: no other languages, scripts, emoji,
    invisible Unicode controls or invalid byte sequences."""
    templated_share: float = 0.15               # one of 30 canned descriptions (S33)
    long_text_share: float = 0.002              # 20k-100k characters
    very_long_text_share: float = 0.0002        # 100k-200k characters
    empty_description_share: float = 0.005      # subject present, description blank (S12)
    html_share: float = 0.01
    llm_problem_share: float = 0.015            # ticket-level (S30/S31)
    agent_category_noise: float = 0.30


@dataclass(frozen=True)
class NestedConfig:
    """Complex-type content (S34): helpdesk comments (array of structs holding arrays), SLA struct,
    custom-field map and tags array, plus the call-centre IVR_JSON column (JSON text inside CSV)."""
    max_comments: int = 20                      # the export holds the most recent comments only
    attachment_share: float = 0.35              # a customer comment carries 1-3 attachments
    internal_note_share: float = 0.30           # an extra private agent note on the first response
    custom_fields_max: int = 4                  # 0..max keys per ticket (variable key set -> map)
    duplicate_comment_share: float = 0.01       # one comment element repeated (export retry)
    unordered_comments_share: float = 0.02      # comments array not in chronological order
    size_as_text_share: float = 0.005           # an attachment size_bytes delivered as a string
    dirty_tags_share: float = 0.02              # duplicate, padded, upper-case or null tag elements
    null_arrays_share: float = 0.01             # tags or comments explicitly null
    custom_field_drift_share: float = 0.01      # key case drift ('Plan') or a null map value
    null_author_share: float = 0.005            # a comment whose author struct is null
    ivr_bad_json_share: float = 0.005           # IVR_JSON not valid JSON (legacy single quotes)


@dataclass(frozen=True)
class PipelineContract:
    """Pipeline behaviour the expectations assume (PDF 1, Sections 9 and 17). Keep in sync."""
    max_malformed_ratio: float = 0.05           # file REJECTED above this share of unparseable rows
    snapshot_min_ratio_of_active: float = 0.9   # DV-8: smaller CRM snapshots are PARTIAL
    future_tolerance_hours: int = 24            # R-TKT-007 upper bound: as_of + tolerance
    max_description_chars: int = 20_000         # R-TKT-013
    batch_fail_pct: tuple[tuple[str, float], ...] = (
        ("R-TKT-001", 5.0), ("R-TKT-002", 20.0), ("R-TKT-003", 20.0), ("R-TKT-004", 20.0),
        ("R-TKT-005", 20.0), ("R-TKT-006", 20.0), ("R-TKT-007", 10.0),
        ("R-CUS-001", 5.0), ("R-CUS-002", 5.0))


@dataclass(frozen=True)
class WarehouseConfig:
    """Data-modelling sources: CRM customer-profile CDC feed (S35), product catalog and agent roster
    dimensions (S36), billing account key map and early-arriving facts (S37)."""
    cdc_profile_share: float = 0.20             # share of CRM customers that have a profile in the CDC source
    cdc_update_share: float = 0.03              # profiles updated per day
    cdc_delete_share: float = 0.002             # profiles hard-deleted per day
    cdc_reinsert_share: float = 0.40            # deleted profiles re-created a few days later
    cdc_orphan_creates_per_day: int = 1         # creates for ids that never exist in CRM
    cdc_late_event_share: float = 0.02          # event delivered in the next day's file
    cdc_duplicate_event_share: float = 0.01     # event delivered twice
    cdc_noop_updates_per_day: int = 2           # update events that change nothing
    cdc_same_ts_pairs_per_day: int = 1          # two updates of one key with the same commit_ts
    cdc_backdated_per_day: int = 1              # effective_from earlier than the commit date
    agent_transfer_share: float = 0.06
    agent_double_transfer_share: float = 0.01
    agent_overlap_share: float = 0.0075
    agent_gap_share: float = 0.0075
    agent_two_open_share: float = 0.005
    agent_leaver_share: float = 0.01
    agent_rehire_share: float = 0.005
    billing_second_account_share: float = 0.20
    billing_third_account_share: float = 0.05
    billing_no_account_share: float = 0.30
    billing_orphan_share: float = 0.005
    billing_blank_customer_share: float = 0.002
    billing_key_format_share: float = 0.01
    billing_moved_share: float = 0.005
    billing_duplicate_row_share: float = 0.003
    billing_shared_account_share: float = 0.002
    early_fact_share: float = 0.01              # tickets/calls of the last days naming a not-yet-delivered customer


@dataclass(frozen=True)
class ScenarioConfig:
    s21_malformed_ratio: float = 0.02
    s21_every_nth_file: int = 25
    s22_malformed_ratio: float = 0.12
    s23_count_delta: int = 7
    scenario_file_share: float = 0.10           # share of a slot moved into its scenario file
    scenario_file_min_records: int = 40
    late_versions_share: float = 0.01           # S17, relative to daily tickets
    conflict_pairs_share: float = 0.003         # S18
    correction_share: float = 0.03              # S20, relative to one call-centre day
    s27_csat_text_share: float = 1.0            # a type change hits every non-null csat in the file
    backlog_days: int = 20                      # S28 re-export window
    backlog_share: float = 0.25                 # S28 rows, relative to daily calls
    crm_partial_ratio: float = 0.60             # S32
    crm_duplicate_share: float = 0.0003
    crm_bad_tier_share: float = 0.0005
    crm_bad_signup_share: float = 0.0005
    crm_bad_id_share: float = 0.0001
    crm_blank_id_share: float = 0.0001
    golden_set_size: int = 200


@dataclass(frozen=True)
class GeneratorConfig:
    profile: VolumeProfile
    seed: int = 20260915
    anchor_date: date = date(2026, 9, 15)
    mix: RecordMix = field(default_factory=RecordMix)
    skew: SkewConfig = field(default_factory=SkewConfig)
    text: TextConfig = field(default_factory=TextConfig)
    nested: NestedConfig = field(default_factory=NestedConfig)
    contract: PipelineContract = field(default_factory=PipelineContract)
    scenarios: ScenarioConfig = field(default_factory=ScenarioConfig)
    warehouse: WarehouseConfig = field(default_factory=WarehouseConfig)
    # --- what is written and where (not part of the fingerprint) ---
    batches: tuple[str, ...] = ALL_BATCHES
    sources: tuple[str, ...] = ALL_SOURCES
    landing_root: Path = Path("Input/landing")
    expected_root: Path = Path("Input/testdata/expected")
    validate: bool = True

    _NOT_FINGERPRINTED = ("batches", "sources", "landing_root", "expected_root", "validate")

    def content_dict(self) -> dict:
        data = dataclasses.asdict(self)
        for key in self._NOT_FINGERPRINTED:
            data.pop(key, None)
        return data

    def fingerprint(self) -> str:
        canonical = json.dumps(self.content_dict(), sort_keys=True, default=str, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
