"""Tests for the generator itself - standard library only.

    python -m unittest discover -s tests -v          (from the folder that contains data_generator/)

The end-to-end class generates the small profile once (about 5 seconds) into a temporary folder.
"""
from __future__ import annotations

import csv
import dataclasses
import gzip
import json
import tempfile
import unittest
from pathlib import Path

from data_generator.config.loader import ConfigError, build_config
from data_generator.core.rng import Weighted, Zipf, stream, unit
from data_generator.labels.outcomes import llm_expected, warn_hits
from data_generator.main import generate
from data_generator.scenarios.catalog import SCENARIOS
from data_generator.validators.determinism import tree_fingerprint
from data_generator.validators.self_check import verify
from data_generator.writers.layout import ExpectedLayout


class RandomStreams(unittest.TestCase):
    def test_streams_are_reproducible_and_independent(self):
        first = stream(7, "helpdesk", "day", "2026-09-01")
        again = stream(7, "helpdesk", "day", "2026-09-01")
        other = stream(7, "helpdesk", "day", "2026-09-02")
        numbers = [first.random() for _ in range(5)]
        self.assertEqual(numbers, [again.random() for _ in range(5)])
        self.assertNotEqual(numbers, [other.random() for _ in range(5)])

    def test_unit_is_stable_and_in_range(self):
        values = [unit(1, "cust", n, "tier") for n in range(1_000)]
        self.assertTrue(all(0.0 <= v < 1.0 for v in values))
        self.assertEqual(values, [unit(1, "cust", n, "tier") for n in range(1_000)])

    def test_weighted_and_zipf_follow_their_weights(self):
        rng = stream(1, "test")
        weighted = Weighted([("a", 90.0), ("b", 10.0), ("never", 0.0)])
        picks = [weighted.pick(rng) for _ in range(20_000)]
        self.assertAlmostEqual(picks.count("a") / len(picks), 0.9, delta=0.01)
        self.assertNotIn("never", picks)
        zipf = Zipf(100, 1.0)
        ranks = [zipf.pick(rng) for _ in range(20_000)]
        self.assertGreater(ranks.count(1), 1.6 * ranks.count(2))        # expected ratio: 2


class Configuration(unittest.TestCase):
    def test_invalid_overrides_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            mix = Path(tmp) / "mix.json"
            mix.write_text(json.dumps({"mix": {"valid": 50.0}}), encoding="utf-8")
            with self.assertRaisesRegex(ConfigError, "sum to 100"):
                build_config("small", mix)
            typo = Path(tmp) / "typo.json"
            typo.write_text(json.dumps({"skew": {"hot_customer_shares": 0.2}}), encoding="utf-8")
            with self.assertRaisesRegex(ConfigError, "unknown setting"):
                build_config("small", typo)

    def test_fingerprint_covers_content_not_locations(self):
        a = build_config("small", landing_root=Path("/a"))
        b = build_config("small", landing_root=Path("/b"), batches=("B001",))
        self.assertEqual(a.fingerprint(), b.fingerprint())
        self.assertNotEqual(a.fingerprint(), build_config("small", seed=1).fingerprint())


class ExpectationRules(unittest.TestCase):
    def test_warn_hits(self):
        self.assertEqual(warn_hits("C0000000", "LOGIN-SSO", "text", 20_000), ["R-TKT-015"])
        self.assertEqual(warn_hits(" CUST-9 ", "NEW-PRODUCT-X", "  ", 20_000),
                         ["R-TKT-012", "R-TKT-014", "R-TKT-016"])
        self.assertEqual(warn_hits(None, "LOGIN-SSO", "x" * 20_001, 20_000), ["R-TKT-013"])

    def test_llm_expected(self):
        self.assertEqual(llm_expected(None, " \n "), "SKIPPED")
        self.assertEqual(llm_expected("s", "text [[FAULT:INVALID_JSON]]"), "PERMANENT_FAILURE:INVALID_JSON")
        self.assertEqual(llm_expected("s", "text [[FAULT:TIMEOUT_UNTIL_ATTEMPT=2]]"), "SUCCESS_AFTER_RETRY")
        self.assertEqual(llm_expected("s", "plain text"), "SUCCESS")


class SmallProfileEndToEnd(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory(prefix="datagen-test-")
        root = Path(cls._tmp.name)
        cls.cfg = build_config("small", landing_root=root / "landing", expected_root=root / "expected")
        cls.batches = {b["batch"]: b for b in generate(cls.cfg)["batches"]}
        cls.expected = ExpectedLayout(cls.cfg.expected_root, "small")

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def _file_rows(self) -> list[dict]:
        rows = []
        for batch in self.batches:
            path = self.expected.batch_dir(batch) / "file_expectations.csv"
            rows += list(csv.DictReader(path.read_text(encoding="utf-8").splitlines()))
        return rows

    def test_self_check_passes(self):
        self.assertEqual(verify(self.cfg), [])

    def test_batch_plan_outcomes(self):
        b = self.batches
        self.assertEqual(b["B001"]["files"]["by_status"], {"LOADED": b["B001"]["files"]["total"]})
        self.assertEqual(b["B003"]["files"]["by_status"].get("DUPLICATE"), 1)          # S19
        self.assertGreaterEqual(b["B003"]["rows"]["stale_ignored"], 20)                # S17
        self.assertEqual(sorted(b["B004"]["files"]["rejected"].values()), [
            "CONTROL_FILE_MISSING", "CONTROL_MISMATCH", "MALFORMED_RATIO",
            "MISSING_REQUIRED_FIELD:created_at", "MISSING_REQUIRED_FIELD:priority", "PARTIAL_SNAPSHOT"])
        self.assertNotIn("REJECTED", b["B005"]["files"]["by_status"])                  # recovery
        self.assertEqual(b["B006"]["files"]["by_status"], {"DUPLICATE": 2})            # replays (tickets, CDC)
        self.assertEqual(b["B006"]["rows"]["in_loaded_files"], 0)
        for batch in b.values():
            self.assertEqual(batch["threshold_breaches"], [], batch["batch"])

    def test_volumes_are_near_the_profile_targets(self):
        totals: dict[str, int] = {}
        for row in self._file_rows():
            totals[row["source"]] = totals.get(row["source"], 0) + int(row["rows"])
        self.assertTrue(35_000 <= totals["helpdesk"] <= 45_000, totals)
        self.assertTrue(8_500 <= totals["callcenter"] <= 11_500, totals)

    def test_every_catalogued_scenario_is_generated(self):
        seen = set()
        for row in self._file_rows():
            seen.update(s[:4].rstrip("-") for s in row["scenarios"].split(";") if s)
        for batch in self.batches:
            with gzip.open(self.expected.labels(batch), "rt", encoding="utf-8") as fh:
                for line in fh:
                    seen.update(s.split(":")[0] for s in json.loads(line)["scenarios"])
        seen = {s.rstrip("AB") if s.startswith("S23") else s for s in seen}
        self.assertEqual(set(SCENARIOS) - seen, set())

    def test_landing_files_are_ascii(self):
        for path in Path(self.cfg.landing_root).rglob("*"):
            if path.is_file():
                path.read_bytes().removeprefix(b"\xef\xbb\xbf").decode("ascii")

    def test_helpdesk_records_carry_complex_types(self):
        path = next(Path(self.cfg.landing_root, "helpdesk").rglob("*.jsonl"))
        records = []
        for line in path.read_text(encoding="ascii").splitlines():
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        with_comments = [r for r in records if r.get("comments")]
        self.assertTrue(with_comments)
        comment = with_comments[0]["comments"][0]
        self.assertEqual(set(comment) - {"attachments"},
                         {"comment_id", "author", "created_at", "public", "body"})
        sla = next(r["sla"] for r in records if r.get("sla"))
        self.assertEqual(set(sla), {"policy", "targets", "pause_windows"})
        self.assertTrue(any(isinstance(r.get("custom_fields"), dict) for r in records))

    def test_cdc_feed_replays_to_the_answer_key(self):
        events = {}
        for path in Path(self.cfg.landing_root, "crm", "customer_changes").rglob("*.jsonl"):
            for line in path.read_text(encoding="ascii").splitlines():
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                events[event["event_id"]] = event
        state = {}
        for event in sorted(events.values(), key=lambda e: e["source_seq"]):
            if event["op"] == "d":
                state.pop(event["customer_id"], None)
            else:
                state[event["customer_id"]] = event["after"]
        truth_path = self.expected.warehouse / "cdc_customer_profile_final.csv"
        truth = {r["customer_id"]: r for r in csv.DictReader(truth_path.read_text(encoding="ascii").splitlines())}
        self.assertEqual(set(state), set(truth))
        for cid, after in state.items():
            self.assertEqual({k: "" if v is None else str(v) for k, v in after.items()},
                             {k: v for k, v in truth[cid].items() if k != "customer_id"})

    def test_reference_and_billing_sources_exist(self):
        root = Path(self.cfg.landing_root)
        self.assertEqual(len(list((root / "reference" / "products").rglob("*.csv"))), 3)
        self.assertEqual(len(list((root / "reference" / "agents").rglob("*.csv"))), 2)
        self.assertEqual(len(list((root / "billing" / "account_map").rglob("*.csv"))), 2)

    def test_partial_generation_reproduces_the_same_files(self):
        """Writing only B003 simulates B001-B002 without writing them; B003 must be byte-identical."""
        with tempfile.TemporaryDirectory() as tmp:
            again = dataclasses.replace(self.cfg, batches=("B003",), landing_root=Path(tmp) / "landing",
                                        expected_root=Path(tmp) / "expected", validate=False)
            generate(again)
            partial = tree_fingerprint(again.landing_root)
        full = tree_fingerprint(self.cfg.landing_root)
        self.assertTrue(partial)
        for path, digest in partial.items():
            self.assertEqual(full.get(path), digest, path)


if __name__ == "__main__":
    unittest.main()
