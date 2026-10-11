"""Hands-on PySpark practice-data generator - command line entry point.

    python -m data_generator --profile small --out Input --clean
    python -m data_generator --profile medium --batches B001,B002 --only-source helpdesk,crm

Batches are always simulated in order up to the last requested one (later batches depend on
earlier ones); only requested batches are written.
"""
from __future__ import annotations

import argparse
import logging
import platform
import shutil
import sys
import time
from datetime import date
from pathlib import Path

from . import __version__
from .config.loader import ConfigError, build_config
from .config.settings import ALL_BATCHES, GeneratorConfig
from .feeds.callcenter_feed import CallcenterFeed
from .feeds.crm_feed import CrmFeed
from .feeds.helpdesk_feed import HelpdeskFeed
from .feeds.warehouse_feed import WarehouseFeed
from .generators.customers import CustomerBase
from .generators.text import TextFactory
from .labels.recorder import ExpectationRecorder
from .scenarios.plan import build_plan
from .validators.determinism import verify_determinism
from .validators.self_check import verify
from .writers.layout import ExpectedLayout, LandingLayout

log = logging.getLogger("data_generator")


def generate(cfg: GeneratorConfig) -> dict:
    landing = LandingLayout(cfg.landing_root)
    recorder = ExpectationRecorder(cfg, ExpectedLayout(cfg.expected_root, cfg.profile.name))
    customers = CustomerBase(cfg)
    text = TextFactory(cfg.text)
    crm = CrmFeed(cfg, customers, landing, recorder) if "crm" in cfg.sources else None
    calls = CallcenterFeed(cfg, customers, text, landing, recorder) if "callcenter" in cfg.sources else None
    tickets = HelpdeskFeed(cfg, customers, text, landing, recorder) if "helpdesk" in cfg.sources else None
    warehouse = (WarehouseFeed(cfg, customers, landing, recorder)
                 if {"crm_cdc", "products", "agents", "billing"} & set(cfg.sources) else None)

    plan = build_plan(cfg.anchor_date, cfg.profile.days)
    last = max(ALL_BATCHES.index(b) for b in cfg.batches)
    summaries = []
    for spec in plan[:last + 1]:
        started = time.monotonic()
        write = spec.batch_id in cfg.batches
        recorder.begin_batch(spec.batch_id, write)
        crm_state = crm.emit(spec, write) if crm else None
        if calls:
            calls.emit(spec, write)
        if tickets:
            tickets.emit(spec, write)
        if warehouse:
            warehouse.emit(spec, write)
        summary = recorder.end_batch(spec, crm_state)
        summaries.append(summary)
        log.info("%s %-8s files=%-5d rows=%-9d %s (%.1fs)", spec.batch_id,
                 "written" if write else "simulated", summary["files"]["total"], summary["rows"]["delivered"],
                 summary["files"]["by_status"], time.monotonic() - started)
        if summary["threshold_breaches"]:
            log.warning("%s: batch thresholds exceeded %s - the pipeline run is expected to FAIL",
                        spec.batch_id, summary["threshold_breaches"])
    manifest = {
        "generator_version": __version__, "profile": cfg.profile.name, "seed": cfg.seed,
        "anchor_date": cfg.anchor_date.isoformat(), "config_fingerprint": cfg.fingerprint(),
        "python": f"{sys.version_info.major}.{sys.version_info.minor}",
        "batches_written": list(cfg.batches), "sources": list(cfg.sources),
        "config": cfg.content_dict(),
        "standardisation_contract": STANDARDISATION_CONTRACT,
    }
    if warehouse:
        warehouse.write_truth()
    recorder.finalize(manifest)
    return {"batches": summaries}


STANDARDISATION_CONTRACT = [
    "Strings: trim; empty or whitespace-only -> NULL.",
    "Enums: lower(trim(value)) looked up in the source value map; unmapped values pass through "
    "upper-cased and are caught by ALLOWED_VALUES.",
    "Helpdesk timestamps: yyyy-MM-dd'T'HH:mm:ss[.SSS]XXX | yyyy-MM-dd HH:mm:ss (UTC) | epoch milliseconds.",
    "Call-centre timestamps: dd/MM/yyyy HH:mm in Asia/Kolkata; updated_at = coalesce(CALL_END, CALL_START); "
    "resolved_at = CALL_END when DISPOSITION maps to RESOLVED.",
    "Numbers: try_cast; a non-blank value that cannot be cast becomes NULL and is counted as SYS-CAST.",
    "CSV: header names are normalised (BOM stripped, trimmed, case-insensitive) and matched by name, "
    "never by position; an extra '_overflow' column in the read schema flags rows with too many fields.",
    "MERGE: newest by (updated_at, _delivery_ts, _record_hash); a later delivery with an identical "
    "_record_hash is a no-op.",
    "DQ rules as PDF 1 Section 17.7 plus R-TKT-017 NOT_NULL priority (ERROR), "
    "R-TKT-018 NOT_NULL status (ERROR).",
    "Encoding: every landing file is plain ASCII (UTF-8 compatible, English only); the one drifted "
    "call-centre file (S27) starts with a UTF-8 byte-order mark.",
    "Complex types (S34): helpdesk comments ARRAY<STRUCT<..., author STRUCT, attachments ARRAY<STRUCT>>>, "
    "sla STRUCT<policy, targets ARRAY<STRUCT>, pause_windows ARRAY<ARRAY<STRING>>>, "
    "custom_fields MAP<STRING,STRING> (variable keys), tags ARRAY<STRING>; call-centre IVR_JSON is JSON text "
    "in a CSV column.",
    "CDC (crm/customer_changes): apply events by source_seq after de-duplicating event_id; op r/c = insert, "
    "u = update (empty changed_fields = no-op), d = hard delete; commit_ts is system time, effective_from "
    "is business time. Answer keys: warehouse/cdc_customer_profile_final.csv and _scd2.csv.",
    "Reference: product catalog snapshots (product -> family -> business unit); agent roster full-history "
    "extracts with inclusive effective_from/effective_to dates (empty effective_to = open).",
    "Billing account map: billing_account_id -> customer_id with inclusive valid_from/valid_to (empty = open).",
    "customer_ingestion for a batch completes before ticket_ingestion (R-TKT-015 REFERENCE depends on it).",
]


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="python -m data_generator", description=__doc__.split("\n")[0])
    p.add_argument("--profile", default="small", help="small | medium | large (default: small)")
    p.add_argument("--seed", type=int)
    p.add_argument("--anchor-date", type=date.fromisoformat, help="day D of the timeline, e.g. 2026-09-15")
    p.add_argument("--out", type=Path, default=Path("Input"),
                   help="local root: <out>/landing, <out>/testdata/expected")
    p.add_argument("--landing-root", type=Path, help="overrides <out>/landing (e.g. a Databricks volume)")
    p.add_argument("--expected-root", type=Path, help="overrides <out>/testdata/expected")
    p.add_argument("--batches", help="comma-separated subset of B001..B006 (default: all)")
    p.add_argument("--only-source", help="comma-separated subset of crm,callcenter,helpdesk")
    p.add_argument("--config", type=Path, help="JSON file with setting overrides")
    p.add_argument("--clean", action="store_true", help="delete previous outputs of this profile first")
    p.add_argument("--no-validate", action="store_true", help="skip the self-check")
    p.add_argument("--verify-determinism", action="store_true", help="regenerate and compare all file hashes")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")
    try:
        cfg = build_config(
            args.profile, args.config, seed=args.seed, anchor_date=args.anchor_date,
            batches=tuple(args.batches.split(",")) if args.batches else None,
            sources=tuple(args.only_source.split(",")) if args.only_source else None,
            landing_root=args.landing_root or args.out / "landing",
            expected_root=args.expected_root or args.out / "testdata" / "expected",
            validate=not args.no_validate)
    except (ConfigError, ValueError) as exc:
        log.error("configuration error: %s", exc)
        return 2
    if args.clean:
        for sub in ("helpdesk", "callcenter", "crm", "reference", "billing"):
            shutil.rmtree(Path(cfg.landing_root) / sub, ignore_errors=True)
        shutil.rmtree(Path(cfg.expected_root) / cfg.profile.name, ignore_errors=True)
    log.info("profile=%s seed=%s anchor=%s batches=%s sources=%s python=%s", cfg.profile.name, cfg.seed,
             cfg.anchor_date, ",".join(cfg.batches), ",".join(cfg.sources), platform.python_version())
    started = time.monotonic()
    generate(cfg)
    log.info("generated in %.1fs -> %s", time.monotonic() - started, cfg.landing_root)
    status = 0
    if cfg.validate:
        problems = verify(cfg)
        for problem in problems[:50]:
            log.error("self-check: %s", problem)
        log.info("self-check: %s", "OK" if not problems else f"{len(problems)} problem(s)")
        status = 1 if problems else 0
    if args.verify_determinism:
        diffs = verify_determinism(cfg, generate)
        for diff in diffs[:50]:
            log.error("determinism: %s", diff)
        log.info("determinism: %s", "byte-identical" if not diffs else f"{len(diffs)} difference(s)")
        status = status or (1 if diffs else 0)
    return status


if __name__ == "__main__":
    raise SystemExit(main())
