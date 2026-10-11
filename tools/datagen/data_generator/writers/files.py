"""File writers for the landing zone.

* Atomic: data goes to '<name>.tmp' and is renamed when complete, so a pipeline listing the
  landing folder can never pick up a half-written file.
* Streaming: records are written as they are produced; nothing is buffered per dataset.
* Dry mode: when a batch is simulated but not written (--batches), bytes are only hashed, so
  later batches that depend on earlier ones stay byte-identical.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Sequence

from ..core.timefmt import iso


@dataclass(frozen=True)
class FileStats:
    path: Path
    rows: int
    sha256: str
    size: int
    content: bytes | None = None      # kept only when a later scenario needs to copy the file


class _Sink:
    def __init__(self, path: Path, write: bool, keep_bytes: bool = False):
        self.path, self._write = Path(path), write
        self._hash, self._size, self._buf = hashlib.sha256(), 0, bytearray() if keep_bytes else None
        self._tmp = self.path.with_name(self.path.name + ".tmp")
        self._fh = None
        if write:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._fh = open(self._tmp, "wb")

    def write(self, data: bytes) -> None:
        self._hash.update(data)
        self._size += len(data)
        if self._fh:
            self._fh.write(data)
        if self._buf is not None:
            self._buf.extend(data)

    def close(self, rows: int) -> FileStats:
        if self._fh:
            self._fh.close()
            os.replace(self._tmp, self.path)
        content = bytes(self._buf) if self._buf is not None else None
        return FileStats(self.path, rows, self._hash.hexdigest(), self._size, content)


class JsonlWriter:
    def __init__(self, path: Path, write: bool, keep_bytes: bool = False):
        self._sink, self.rows = _Sink(path, write, keep_bytes), 0

    def write_line(self, line: bytes) -> None:
        self._sink.write(line)
        self.rows += 1

    def close(self) -> FileStats:
        return self._sink.close(self.rows)


def encode_json(record: dict) -> bytes:
    """One JSONL line. ensure_ascii keeps every file pure ASCII even if a non-ASCII value slips in."""
    return json.dumps(record, ensure_ascii=True, separators=(",", ":")).encode("ascii") + b"\n"


class CsvWriter:
    """RFC 4180 CSV (comma, double-quote escaping, CRLF), ASCII, optionally with a UTF-8 BOM (S27)."""

    def __init__(self, path: Path, header: Sequence[str], write: bool, bom: bool = False):
        self._sink, self.rows = _Sink(path, write), 0
        if bom:
            self._sink.write(b"\xef\xbb\xbf")
        self._emit(self._format(header))

    @staticmethod
    def _format(values: Sequence[str]) -> str:
        buf = io.StringIO()
        csv.writer(buf, lineterminator="\r\n").writerow(values)
        return buf.getvalue()

    def _emit(self, text: str) -> None:
        self._sink.write(text.encode("ascii"))           # the practice data is ASCII-only by contract

    def write_row(self, values: Sequence[str]) -> None:
        self._emit(self._format(values))
        self.rows += 1

    def write_raw(self, line: str) -> None:
        self._emit(line + "\r\n")
        self.rows += 1

    def close(self) -> FileStats:
        return self._sink.close(self.rows)


def write_control(data: FileStats, delivery: datetime, schema_version: str, write: bool,
                  count_delta: int = 0) -> FileStats:
    """Sidecar control file: record count and SHA-256 of the data file (DV-2, DV-3)."""
    body = json.dumps({"file_name": data.path.name, "record_count": data.rows + count_delta,
                       "sha256": data.sha256, "generated_at": iso(delivery + timedelta(seconds=7)),
                       "schema_version": schema_version}) + "\n"
    sink = _Sink(data.path.with_suffix(".ctl.json"), write, keep_bytes=True)
    sink.write(body.encode("utf-8"))
    return sink.close(1)


def write_bytes(path: Path, content: bytes, rows: int, write: bool) -> FileStats:
    sink = _Sink(path, write)
    sink.write(content)
    return sink.close(rows)
