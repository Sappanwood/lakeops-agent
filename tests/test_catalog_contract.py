import json
from pathlib import Path
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = REPO_ROOT / "data" / "catalog" / "catalog.json"


class CatalogContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.catalog = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))

    def test_wikimedia_sources_cover_batch_and_streaming(self) -> None:
        self.assertEqual(self.catalog["domain"], "wikimedia")
        sources = {source["id"]: source for source in self.catalog["sources"]}

        pageviews = sources["wikimedia_pageviews"]
        self.assertEqual(pageviews["delivery"], "hourly_files")
        self.assertEqual(pageviews["daily_batch"]["expected_hourly_files"], 24)
        self.assertEqual(pageviews["daily_batch"]["timezone"], "UTC")
        self.assertIn("D+1 000000", pageviews["daily_batch"]["source_file_set"])
        self.assertIn("capture-end", pageviews["url_template_clock"])

        recentchange = sources["wikimedia_recentchange"]
        self.assertEqual(recentchange["delivery"], "server_sent_events")
        self.assertTrue(recentchange["endpoint"].startswith("https://"))

    def test_volume_profiles_include_hundreds_of_mb_and_gb_scales(self) -> None:
        profiles = {profile["id"]: profile for profile in self.catalog["volume_profiles"]}
        self.assertGreaterEqual(profiles["demo"]["expected_compressed_mb"][0], 250)
        self.assertGreaterEqual(profiles["daily"]["expected_compressed_mb"][0], 1024)
        self.assertEqual(profiles["daily"]["pageview_hours"], 24)

    def test_query_surface_excludes_personal_recentchange_fields(self) -> None:
        privacy = self.catalog["privacy"]
        forbidden = set(privacy["forbidden_persisted_stream_fields"])
        self.assertTrue({"user", "comment", "parsedcomment", "log_params"} <= forbidden)

        datasets = {dataset["id"]: dataset for dataset in self.catalog["datasets"]}
        stream_fields = set(datasets["recentchange_events"]["fields"])
        self.assertTrue(forbidden.isdisjoint(stream_fields))

        query_views = set(self.catalog["query_surface"]["views"])
        self.assertNotIn("recentchange_events", query_views)

    def test_contract_has_no_telecom_domain_residue(self) -> None:
        serialized = json.dumps(self.catalog).lower()
        for forbidden_term in ("telecom", "ofcom", "cell site", "service telemetry"):
            self.assertNotIn(forbidden_term, serialized)

    def test_primary_scenario_repairs_a_missing_hour_without_overwrite(self) -> None:
        scenario = self.catalog["primary_scenario"]
        self.assertEqual(scenario["fault"], "missing_hourly_pageviews_partition")
        self.assertEqual(scenario["remediation"]["operation"], "backfill_hour")
        self.assertTrue(scenario["remediation"]["requires_approval"])
        self.assertTrue(scenario["remediation"]["publishes_new_manifest"])
        self.assertFalse(scenario["remediation"]["overwrites_existing_objects"])
        self.assertEqual(scenario["withheld_source_label"], "20260801-140000")

    def test_cross_source_join_has_explicit_identity_normalization(self) -> None:
        normalization = self.catalog["identity_normalization"]
        self.assertEqual(
            normalization["stream_project_allowlist"]["en.wikipedia.org"],
            "en",
        )
        self.assertIn("canonical project_code", normalization["join_rule"])
        self.assertIn("canonical page_title", normalization["join_rule"])
        self.assertIn("already URL-decoded canonical DBkey", normalization["page_title_rule"])
        self.assertIn("preserve literal percent", normalization["page_title_rule"])


if __name__ == "__main__":
    unittest.main()
