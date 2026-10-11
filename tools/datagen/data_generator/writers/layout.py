"""Landing-zone and expectation paths. The same code writes to a local folder or to a Databricks
volume path such as /Volumes/<catalog>/<schema>/<volume>/landing."""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from ..core.timefmt import compact


class LandingLayout:
    def __init__(self, root: Path):
        self.root = Path(root)

    @staticmethod
    def _arrival(delivery: datetime) -> str:
        return f"arrival_date={delivery:%Y-%m-%d}"

    def helpdesk(self, delivery: datetime, seq: int) -> Path:
        return (self.root / "helpdesk" / "tickets" / self._arrival(delivery)
                / f"helpdesk_tickets_{compact(delivery)}_{seq:04d}.jsonl")

    @staticmethod
    def control_for(data_file: Path) -> Path:
        return data_file.with_suffix(".ctl.json")

    def callcenter(self, business_date: date, delivery: datetime) -> Path:
        return (self.root / "callcenter" / "tickets" / self._arrival(delivery)
                / f"callcenter_calls_{business_date:%Y%m%d}_{compact(delivery)}.csv")

    def crm(self, snapshot_date: date, delivery: datetime) -> Path:
        return (self.root / "crm" / "customers" / self._arrival(delivery)
                / f"crm_customers_{snapshot_date:%Y%m%d}_{compact(delivery)}.csv")

    def crm_cdc(self, delivery: datetime, seq: int = 1) -> Path:
        return (self.root / "crm" / "customer_changes" / self._arrival(delivery)
                / f"crm_customer_changes_{compact(delivery)}_{seq:04d}.jsonl")

    def products(self, snapshot_date: date, delivery: datetime) -> Path:
        return (self.root / "reference" / "products" / self._arrival(delivery)
                / f"product_catalog_{snapshot_date:%Y%m%d}_{compact(delivery)}.csv")

    def agents(self, as_of: date, delivery: datetime) -> Path:
        return (self.root / "reference" / "agents" / self._arrival(delivery)
                / f"agent_roster_{as_of:%Y%m%d}_{compact(delivery)}.csv")

    def billing(self, as_of: date, delivery: datetime) -> Path:
        return (self.root / "billing" / "account_map" / self._arrival(delivery)
                / f"billing_account_map_{as_of:%Y%m%d}_{compact(delivery)}.csv")

    def relative(self, path: Path) -> str:
        return path.relative_to(self.root).as_posix()


class ExpectedLayout:
    def __init__(self, root: Path, profile: str):
        self.dir = Path(root) / profile

    def batch_dir(self, batch: str) -> Path:
        return self.dir / batch

    @property
    def manifest(self) -> Path:
        return self.dir / "generation_manifest.json"

    def labels(self, batch: str) -> Path:
        return self.dir / batch / "record_labels.jsonl.gz"

    @property
    def warehouse(self) -> Path:
        return self.dir / "warehouse"

    @property
    def golden(self) -> Path:
        return self.dir / "llm_golden.jsonl"
