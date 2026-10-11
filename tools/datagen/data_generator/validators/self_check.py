"""Self-check: re-reads every written file with an independent, deliberately simple parser and
compares what it finds with the expectations the generator wrote. A generator whose ground truth
does not match its own files would make every pipeline test meaningless."""
from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
from collections import Counter
from pathlib import Path

from ..config.settings import GeneratorConfig
from ..writers.layout import ExpectedLayout


def verify(cfg: GeneratorConfig) -> list[str]:
    expected = ExpectedLayout(cfg.expected_root, cfg.profile.name)
    root = Path(cfg.landing_root)
    problems: list[str] = []
    for batch in cfg.batches:
        labels = _load_labels(expected.labels(batch)) if cfg.profile.record_labels else None
        fe_path = expected.batch_dir(batch) / "file_expectations.csv"
        if not fe_path.exists():
            problems.append(f"{batch}: missing {fe_path}")
            continue
        rows = list(csv.DictReader(io.StringIO(fe_path.read_text(encoding="utf-8"))))
        for row in rows:
            problems += _check_file(root, row, labels)
        problems += _check_batch(batch, rows, expected.batch_dir(batch) / "batch_expectations.json")
    return problems


def _check_batch(batch: str, rows: list[dict], path: Path) -> list[str]:
    """The batch summary must be the sum of its file rows (two independently written artifacts)."""
    summary = json.loads(path.read_text(encoding="utf-8"))
    loaded = [r for r in rows if r["expected_status"] == "LOADED"]
    totals = {"delivered": sum(int(r["rows"]) for r in rows),
              "in_loaded_files": sum(int(r["rows"]) for r in loaded),
              "parse_quarantine": sum(int(r["parse_quarantine"]) for r in rows),
              "dq_quarantine": sum(int(r["dq_quarantine"]) for r in rows),
              "nullified": sum(int(r["nullified"]) for r in rows)}
    out = [f"{batch}: rows.{k} is {summary['rows'][k]}, files add up to {v}"
           for k, v in totals.items() if summary["rows"][k] != v]
    hits: Counter = Counter()
    for r in loaded:
        hits.update(json.loads(r["rule_hits"]))
    if dict(hits) != summary["rule_hits"]:
        out.append(f"{batch}: rule_hits differ from the sum of the file rows")
    return out


def _check_file(root: Path, row: dict, labels: dict | None) -> list[str]:
    name, path = row["file"], root / row["file"]
    if not path.exists():
        return [f"{name}: file missing"]
    data = path.read_bytes()
    out: list[str] = []
    try:
        data.removeprefix(b"\xef\xbb\xbf").decode("ascii")       # single, uniform encoding by contract
    except UnicodeDecodeError as exc:
        out.append(f"{name}: non-ASCII byte at offset {exc.start}")
    if path.suffix == ".jsonl":
        rows, parse_bad, truncated = _scan_jsonl(data)
    else:
        rows, parse_bad, truncated = _scan_csv(data)
    if rows != int(row["rows"]):
        out.append(f"{name}: {rows} rows, expected {row['rows']}")
    if row["expected_status"] == "LOADED" and parse_bad != int(row["parse_quarantine"]):
        out.append(f"{name}: {parse_bad} unparseable rows, expected {row['parse_quarantine']}")
    ctl = path.with_suffix(".ctl.json")
    if row["control_file"] in ("present", "mismatch"):
        if not ctl.exists():
            out.append(f"{name}: control file missing")
        else:
            meta = json.loads(ctl.read_text(encoding="utf-8"))
            if meta["sha256"] != hashlib.sha256(data).hexdigest():
                out.append(f"{name}: control sha256 does not match")
            count_ok = meta["record_count"] == rows
            if count_ok != (row["control_file"] == "present"):
                out.append(f"{name}: control record_count {meta['record_count']} vs {rows} rows "
                           f"(control_file={row['control_file']})")
    elif row["control_file"] == "missing" and ctl.exists():
        out.append(f"{name}: control file should be missing")
    if labels is not None:
        got = labels.get(name, Counter())
        if got["rows"] != rows:
            out.append(f"{name}: {got['rows']} record labels for {rows} rows")
        if row["expected_status"] == "LOADED" and got["TRUNCATED_ROW"] != truncated:
            out.append(f"{name}: {truncated} truncated CSV rows, labels say {got['TRUNCATED_ROW']}")
    return out


def _scan_jsonl(data: bytes) -> tuple[int, int, int]:
    lines = data.split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()
    bad = 0
    for line in lines:
        try:
            ok = isinstance(json.loads(line.decode("utf-8", errors="replace")), dict)
        except json.JSONDecodeError:
            ok = False
        bad += not ok
    return len(lines), bad, 0


def _scan_csv(data: bytes) -> tuple[int, int, int]:
    """Counts data rows, rows with too many fields (unquoted delimiter) and too few (truncated)."""
    reader = csv.reader(io.StringIO(data.decode("utf-8-sig", errors="replace"), newline=""))
    header = next(reader)
    rows = overflow = truncated = 0
    for record in reader:
        rows += 1
        overflow += len(record) > len(header)
        truncated += len(record) < len(header)
    return rows, overflow, truncated


def _load_labels(path: Path) -> dict[str, Counter]:
    per_file: dict[str, Counter] = {}
    if not path.exists():
        return per_file
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            lab = json.loads(line)
            c = per_file.setdefault(lab["file"], Counter())
            c["rows"] += 1
            if lab["detail"] == "TRUNCATED_ROW":
                c["TRUNCATED_ROW"] += 1
    return per_file
