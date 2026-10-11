"""Byte-level determinism check: regenerate into a temporary folder and compare every file hash
with the outputs just produced. Costs one extra generation run, so it is opt-in."""
from __future__ import annotations

import dataclasses
import hashlib
import tempfile
from pathlib import Path
from typing import Callable

from ..config.settings import GeneratorConfig


def tree_fingerprint(*roots: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for index, root in enumerate(roots):
        for path in sorted(Path(root).rglob("*")):
            if path.is_file():
                rel = f"{index}/{path.relative_to(root).as_posix()}"
                result[rel] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def verify_determinism(cfg: GeneratorConfig, generate: Callable[[GeneratorConfig], dict]) -> list[str]:
    with tempfile.TemporaryDirectory(prefix="datagen-determinism-") as tmp:
        again = dataclasses.replace(cfg, landing_root=Path(tmp) / "landing",
                                    expected_root=Path(tmp) / "expected", validate=False)
        generate(again)
        first = tree_fingerprint(cfg.landing_root, Path(cfg.expected_root) / cfg.profile.name)
        second = tree_fingerprint(again.landing_root, Path(again.expected_root) / cfg.profile.name)
    problems = [f"only in first run: {p}" for p in sorted(set(first) - set(second))]
    problems += [f"only in second run: {p}" for p in sorted(set(second) - set(first))]
    problems += [f"content differs: {p}" for p in sorted(set(first) & set(second)) if first[p] != second[p]]
    return problems
