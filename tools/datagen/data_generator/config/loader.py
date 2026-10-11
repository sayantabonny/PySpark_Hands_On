"""Builds a validated GeneratorConfig from a profile, an optional JSON override file and CLI values.

Precedence (later wins): profile defaults -> JSON overrides -> CLI flags. JSON is used instead of
YAML so the generator stays standard-library only.
"""
from __future__ import annotations

import dataclasses
import json
import math
import typing
from datetime import date
from pathlib import Path
from typing import Any

from .profiles import PROFILES
from .settings import ALL_BATCHES, ALL_SOURCES, GeneratorConfig


class ConfigError(ValueError):
    """Invalid configuration: the generator stops before writing anything."""


def build_config(profile: str, overrides_file: Path | None = None, **cli: Any) -> GeneratorConfig:
    if profile not in PROFILES:
        raise ConfigError(f"unknown profile '{profile}', expected one of {sorted(PROFILES)}")
    cfg = GeneratorConfig(profile=PROFILES[profile])
    if overrides_file is not None:
        cfg = _merge(cfg, json.loads(Path(overrides_file).read_text(encoding="utf-8")), "config")
    cli_values = {k: v for k, v in cli.items() if v is not None}
    if cli_values:
        cfg = dataclasses.replace(cfg, **cli_values)
    validate(cfg)
    return cfg


def _merge(obj: Any, overrides: dict[str, Any], path: str) -> Any:
    """Recursively applies a JSON dict onto a frozen dataclass; unknown keys are errors."""
    if not isinstance(overrides, dict):
        raise ConfigError(f"{path}: expected an object")
    hints = typing.get_type_hints(type(obj))
    changes: dict[str, Any] = {}
    for key, value in overrides.items():
        if key not in hints:
            raise ConfigError(f"{path}.{key}: unknown setting")
        current = getattr(obj, key)
        if dataclasses.is_dataclass(current):
            changes[key] = _merge(current, value, f"{path}.{key}")
        else:
            changes[key] = _coerce(value, hints[key], f"{path}.{key}")
    return dataclasses.replace(obj, **changes)


def _coerce(value: Any, hint: Any, path: str) -> Any:
    if hint is date:
        return date.fromisoformat(value)
    if hint is Path:
        return Path(value)
    origin = typing.get_origin(hint)
    if origin is tuple:                                   # JSON arrays -> (nested) tuples
        return tuple(tuple(v) if isinstance(v, list) else v for v in value)
    if hint in (int, float) and not isinstance(value, (int, float)):
        raise ConfigError(f"{path}: expected a number")
    return value


def validate(cfg: GeneratorConfig) -> None:
    total = sum(w for _, w in cfg.mix.pairs())
    if not math.isclose(total, 100.0, abs_tol=1e-6):
        raise ConfigError(f"mix percentages must sum to 100, got {total}")
    if any(w < 0 for _, w in cfg.mix.pairs()):
        raise ConfigError("mix percentages must be >= 0")
    for section in (cfg.skew, cfg.text, cfg.nested, cfg.scenarios, cfg.warehouse):
        for f in dataclasses.fields(section):
            v = getattr(section, f.name)
            if f.name.endswith(("_share", "_ratio", "_noise")) and not 0.0 <= v <= 1.0:
                raise ConfigError(f"{type(section).__name__}.{f.name} must be within [0, 1], got {v}")
    if cfg.profile.days < 8:
        raise ConfigError("profile.days must be >= 8 (batches B001-B004 need at least 8 days)")
    if not set(cfg.batches) <= set(ALL_BATCHES) or not cfg.batches:
        raise ConfigError(f"batches must be a non-empty subset of {ALL_BATCHES}")
    if not set(cfg.sources) <= set(ALL_SOURCES) or not cfg.sources:
        raise ConfigError(f"sources must be a non-empty subset of {ALL_SOURCES}")
    if cfg.skew.hot_customers >= cfg.profile.customers // 2:
        raise ConfigError("hot_customers must be well below the customer count")
