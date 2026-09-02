import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from data.samples.wikimedia_fixtures import (
    FixtureValidationError,
    build_fixture_manifest,
    canonical_records,
    load_fixture_profiles,
    publish_fixture_bundle,
    project_recentchange_event,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = REPO_ROOT / "data" / "samples" / "wikimedia"


class WikimediaFixtureTest(unittest.TestCase):
    def test_complete_and_missing_hour_manifests_have_pinned_source_identities(self) -> None:
        complete = build_fixture_manifest("complete_day")
        missing = build_fixture_manifest("missing_hour_day")

        self.assertEqual(complete["partition_date"], "2024-01-01")
        self.assertEqual(len(complete["source_objects"]), 24)
        self.assertEqual(len(missing["source_objects"]), 23)
        self.assertEqual(missing["missing_capture_end"], ["2024-01-01T15:00:00Z"])
        self.assertTrue(
            all(item["upstream_source_url"].startswith("https://dumps.wikimedia.org/") for item in complete["source_objects"])
        )
        self.assertTrue(all(len(item["fixture_sha256"]) == 64 for item in complete["source_objects"]))
        self.assertTrue(all(item["upstream_source_sha256"] is None for item in complete["source_objects"]))
        for scenario, manifest in (("complete_day", complete), ("missing_hour_day", missing)):
            committed = json.loads(
                (FIXTURE_ROOT / "manifests" / f"{scenario}.json").read_text(encoding="utf-8")
            )
            self.assertEqual({key: value for key, value in manifest.items() if key != "profiles"}, committed)

    def test_profiles_select_fixed_hours_and_have_measurable_limits(self) -> None:
        profiles = load_fixture_profiles()
        self.assertEqual(set(profiles), {"tiny", "demo"})
        self.assertLess(profiles["tiny"]["max_source_bytes"], profiles["demo"]["max_source_bytes"])
        self.assertLess(profiles["tiny"]["max_records"], profiles["demo"]["max_records"])
        self.assertGreater(profiles["demo"]["max_runtime_seconds"], 0)

        tiny = build_fixture_manifest("complete_day", profile="tiny")
        demo = build_fixture_manifest("complete_day", profile="demo")
        self.assertEqual(tiny["fixture_measurements"]["pageview_hours"], 1)
        self.assertEqual(demo["fixture_measurements"]["pageview_hours"], 6)
        self.assertLessEqual(tiny["fixture_measurements"]["source_bytes"], profiles["tiny"]["max_source_bytes"])
        self.assertLessEqual(demo["fixture_measurements"]["record_count"], profiles["demo"]["max_records"])

        records = canonical_records(build_fixture_manifest("complete_day"))
        self.assertEqual(records, sorted(records, key=lambda item: (item["capture_end"], item["domain_code"], item["page_title"])))
        self.assertIn("LakeOps_Agent", {record["page_title"] for record in records})

    def test_profile_byte_record_and_runtime_limits_reject_before_publication(self) -> None:
        for limit, value in (("max_source_bytes", 1), ("max_records", 1)):
            with self.subTest(limit=limit), tempfile.TemporaryDirectory() as temporary_directory:
                source_root = self._copied_fixture_root(Path(temporary_directory))
                profiles_path = source_root / "profiles.json"
                profiles = json.loads(profiles_path.read_text(encoding="utf-8"))
                profiles["profiles"]["tiny"][limit] = value
                profiles_path.write_text(json.dumps(profiles), encoding="utf-8")
                destination = Path(temporary_directory) / "published"
                with self.assertRaises(FixtureValidationError):
                    publish_fixture_bundle("complete_day", destination, source_root=source_root, profile="tiny")
                self.assertFalse((destination / "fixture-manifest.json").exists())

        with tempfile.TemporaryDirectory() as temporary_directory:
            destination = Path(temporary_directory) / "published"
            ticks = iter((0.0, 11.0))
            with self.assertRaises(FixtureValidationError):
                publish_fixture_bundle("complete_day", destination, clock=lambda: next(ticks))
            self.assertFalse((destination / "fixture-manifest.json").exists())

    def test_replay_cases_and_projection_reject_privacy_and_scalar_contract_violations(self) -> None:
        manifest = build_fixture_manifest("complete_day")
        cases = manifest["recentchange_replay"]
        self.assertEqual({case["case"] for case in cases}, {"normal", "late", "invalid", "duplicate", "out_of_order"})
        for case in cases:
            self.assertFalse({"user", "comment", "parsedcomment", "log_params", "ip"} & set(case["event"]))

        valid = cases[0]["event"]
        self.assertEqual(project_recentchange_event(valid), valid)
        for mutation in (
            {**valid, "namespace": "0"},
            {**valid, "title": {"text": valid["title"]}},
            {**valid, "user": "not-persisted"},
            {**valid, "meta": {"user": "not-persisted"}},
        ):
            with self.subTest(mutation=mutation):
                with self.assertRaises(FixtureValidationError):
                    project_recentchange_event(mutation)

    def test_invalid_source_or_projection_never_publishes_a_partial_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            destination = Path(temporary_directory) / "published"
            with self.assertRaises(FixtureValidationError):
                publish_fixture_bundle("missing_hour_day", destination)
            self.assertFalse((destination / "fixture-manifest.json").exists())

            with self.assertRaises(FixtureValidationError):
                publish_fixture_bundle("complete_day", destination, inject_projection_failure=True)
            self.assertFalse((destination / "fixture-manifest.json").exists())

    def test_complete_bundle_publishes_once_with_canonical_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            destination = Path(temporary_directory) / "published"
            published = publish_fixture_bundle("complete_day", destination)
            self.assertEqual(
                json.loads(published.read_text(encoding="utf-8")),
                build_fixture_manifest("complete_day", profile="demo"),
            )
            with self.assertRaises(FixtureValidationError):
                publish_fixture_bundle("complete_day", destination)

    def test_hard_link_and_staging_failures_do_not_publish_a_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            destination = Path(temporary_directory) / "published"
            with mock.patch("data.samples.wikimedia_fixtures.os.link", side_effect=OSError("unsupported")):
                with self.assertRaises(FixtureValidationError):
                    publish_fixture_bundle("complete_day", destination)
            self.assertFalse((destination / "fixture-manifest.json").exists())

            with mock.patch("data.samples.wikimedia_fixtures.Path.write_text", side_effect=OSError("disk full")):
                with self.assertRaises(FixtureValidationError):
                    publish_fixture_bundle("complete_day", destination)
            self.assertFalse((destination / "fixture-manifest.json").exists())

    def test_symlink_destination_is_rejected_before_publication(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            real_destination = root / "real"
            real_destination.mkdir()
            destination = root / "linked"
            destination.symlink_to(real_destination, target_is_directory=True)
            with self.assertRaises(FixtureValidationError):
                publish_fixture_bundle("complete_day", destination)
            self.assertFalse((real_destination / "fixture-manifest.json").exists())

    def test_ancestor_symlink_and_existing_final_manifest_symlink_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            real_parent = root / "real"
            real_parent.mkdir()
            linked_parent = root / "linked"
            linked_parent.symlink_to(real_parent, target_is_directory=True)
            with self.assertRaises(FixtureValidationError):
                publish_fixture_bundle("complete_day", linked_parent / "published")
            self.assertFalse((real_parent / "published" / "fixture-manifest.json").exists())

            destination = root / "published"
            destination.mkdir()
            target = root / "existing-manifest"
            target.write_text("do not replace", encoding="utf-8")
            (destination / "fixture-manifest.json").symlink_to(target)
            with self.assertRaises(FixtureValidationError):
                publish_fixture_bundle("complete_day", destination)
            self.assertEqual(target.read_text(encoding="utf-8"), "do not replace")

    def test_concurrent_publishers_have_normal_no_clobber_behavior(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            destination = Path(temporary_directory) / "published"

            def publish() -> str:
                try:
                    publish_fixture_bundle("complete_day", destination)
                except FixtureValidationError:
                    return "rejected"
                return "published"

            with ThreadPoolExecutor(max_workers=2) as executor:
                outcomes = list(executor.map(lambda _: publish(), range(2)))
            self.assertEqual(sorted(outcomes), ["published", "rejected"])
            self.assertTrue((destination / "fixture-manifest.json").is_file())

    def test_source_tampering_and_mixed_checksum_provenance_are_detected_before_publication(self) -> None:
        for mutation in ("source", "missing_fixture_checksum", "mixed_upstream_checksum"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary_directory:
                source_root = self._copied_fixture_root(Path(temporary_directory))
                if mutation == "source":
                    records_path = source_root / "pageviews" / "complete-day.json"
                    records_path.write_text(
                        records_path.read_text(encoding="utf-8").replace("LakeOps_Agent", "Changed_Title", 1),
                        encoding="utf-8",
                    )
                else:
                    manifest_path = source_root / "manifests" / "complete_day.json"
                    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                    if mutation == "missing_fixture_checksum":
                        del manifest["source_objects"][0]["fixture_sha256"]
                    else:
                        manifest["source_objects"][0]["upstream_source_sha256"] = "0" * 64
                    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

                destination = Path(temporary_directory) / "published"
                with self.assertRaises(FixtureValidationError):
                    publish_fixture_bundle("complete_day", destination, source_root=source_root)
                self.assertFalse((destination / "fixture-manifest.json").exists())

    @staticmethod
    def _copied_fixture_root(temporary_directory: Path) -> Path:
        source_root = temporary_directory / "fixtures"
        source_root.mkdir()
        for path in FIXTURE_ROOT.rglob("*.json"):
            copied = source_root / path.relative_to(FIXTURE_ROOT)
            copied.parent.mkdir(parents=True, exist_ok=True)
            copied.write_bytes(path.read_bytes())
        return source_root


if __name__ == "__main__":
    unittest.main()
