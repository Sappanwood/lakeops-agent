"""End-to-end governed SQL evidence and failure contracts."""

import gzip
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from agent.query_adapter import GovernedSqlAdapter, QueryAdapterError
from agent.query_safety import QueryLimits
from pipelines.batch.gold import materialize_fixture_ingestion_freshness, materialize_project_traffic_daily
import test_gold_metrics


class QueryAdapterTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.manifest = materialize_fixture_ingestion_freshness(
            "complete_day", self.root, run_id="complete"
        )

    def adapter(self, **kwargs):
        return GovernedSqlAdapter(self.manifest, self.root, **kwargs)

    def test_correct_answer_has_attributable_bounded_evidence(self):
        result = self.adapter().execute("SELECT expected_count, accepted_count FROM v_ingestion_freshness")
        self.assertEqual(result.rows, [(24, 24)])
        self.assertEqual(result.row_count, 1)
        self.assertEqual(result.source_datasets, ("ingestion_freshness",))
        self.assertEqual(result.data_status, "complete")
        self.assertEqual(result.resource_policy.outcome, "passed")
        self.assertEqual(result.resource_policy.limits, QueryLimits())
        self.assertGreater(result.elapsed_seconds, 0)
        self.assertGreater(result.resource_policy.scan_bytes_upper_bound, 0)
        self.assertEqual(result.sources[0].manifest_sha256, hashlib.sha256(self.manifest.read_bytes()).hexdigest())
        self.assertEqual(result.sources[0].manifest_id, "complete")
        self.assertTrue(result.sources[0].partition_date)
        self.assertIn("v_ingestion_freshness", result.sql)

    def test_empty_success_missing_and_stale_are_distinct(self):
        empty = self.adapter().execute("SELECT * FROM v_ingestion_freshness WHERE accepted_count = 0")
        self.assertEqual(empty.rows, [])
        self.assertEqual(empty.data_status, "complete")
        self.manifest = materialize_fixture_ingestion_freshness("missing_hour_day", self.root, run_id="missing")
        missing = self.adapter().execute("SELECT accepted_count FROM v_ingestion_freshness WHERE accepted_count = 0")
        self.assertEqual(missing.rows, [])
        self.assertEqual(missing.data_status, "missing")
        with self.assertRaises(QueryAdapterError) as stale:
            self.adapter(required_partition_date="2099-01-01").execute("SELECT * FROM v_ingestion_freshness")
        self.assertEqual(stale.exception.code, "stale_data")
        with self.assertRaises(QueryAdapterError) as unavailable:
            self.adapter().execute("SELECT * FROM v_project_traffic_daily")
        self.assertEqual(unavailable.exception.code, "view_unavailable")

    def test_rejected_access_execution_failure_and_limits(self):
        cases = [
            ("SELECT * FROM read_parquet('/tmp/private')", {}, "unknown_view"),
            ("SELECT * FROM ingestion_freshness", {}, "unknown_view"),
            ("SELECT secret FROM v_ingestion_freshness", {}, "query_failure"),
            ("SELECT * FROM v_ingestion_freshness", {"limits": QueryLimits(max_result_bytes=1)}, "result_size_limit"),
            ("SELECT * FROM v_ingestion_freshness", {"limits": QueryLimits(max_scan_bytes=1)}, "scan_limit"),
        ]
        for sql, options, code in cases:
            with self.subTest(code=code), self.assertRaises(QueryAdapterError) as caught:
                self.adapter(**options).execute(sql)
            self.assertEqual(caught.exception.code, code)
            self.assertNotIn(str(self.root), str(caught.exception))

    def test_manifest_paths_reject_static_symlinks_and_escape(self):
        alias = self.root / "alias.json"
        alias.symlink_to(self.manifest)
        for manifest in (alias, self.root / ".." / "outside.json"):
            with self.subTest(manifest=manifest), self.assertRaises(QueryAdapterError) as caught:
                GovernedSqlAdapter(manifest, self.root).execute("SELECT * FROM v_ingestion_freshness")
            self.assertEqual(caught.exception.code, "unsafe_input")

    def test_tampered_catalog_and_changed_or_missing_evidence_fail_closed(self):
        catalog = json.loads(Path("data/catalog/catalog.json").read_text())
        catalog["query_surface"]["views"].append("attacker")
        path = self.root / "catalog.json"
        path.write_text(json.dumps(catalog))
        with self.assertRaises(QueryAdapterError) as invalid:
            self.adapter(catalog_path=path)
        self.assertEqual(invalid.exception.code, "catalog_contract_failure")
        adapter = self.adapter()
        doc = json.loads(self.manifest.read_text())
        output = self.root / doc["output_objects"][0]["object_path"]
        output.write_bytes(output.read_bytes() + b"changed")
        with self.assertRaises(QueryAdapterError) as changed:
            adapter.execute("SELECT * FROM v_ingestion_freshness")
        self.assertEqual(changed.exception.code, "broken_freshness_join")
        self.manifest.unlink()
        with self.assertRaises(QueryAdapterError) as missing:
            adapter.execute("SELECT * FROM v_ingestion_freshness")
        self.assertEqual(missing.exception.code, "missing_data")

    def test_structurally_valid_unsupported_view_sources_fail_closed(self):
        from data.catalog.validator import validate_catalog

        cases = (
            ("v_ingestion_freshness", ["project_traffic_daily"], [], ["partition_date"]),
            ("v_project_traffic_daily", ["ingestion_freshness"], [], ["partition_date"]),
            ("v_project_traffic_daily", ["pageviews_hourly", "page_activity_hourly"],
             ["page_traffic_activity_by_hour"], ["project_code"]),
        )
        for view, inputs, joins, fields in cases:
            with self.subTest(view=view, inputs=inputs):
                catalog = json.loads(Path("data/catalog/catalog.json").read_text())
                catalog["query_surface"]["view_contracts"][view] = {
                    "inputs": inputs, "joins": joins, "fields": fields,
                }
                validate_catalog(catalog)
                path = self.root / "catalog.json"
                path.write_text(json.dumps(catalog))
                with self.assertRaises(QueryAdapterError) as caught:
                    self.adapter(catalog_path=path).execute("SELECT * FROM v_ingestion_freshness")
                self.assertEqual(caught.exception.code, "catalog_contract_failure")

    def test_catalog_field_subset_remains_the_projection_authority(self):
        catalog = json.loads(Path("data/catalog/catalog.json").read_text())
        catalog["query_surface"]["view_contracts"]["v_ingestion_freshness"]["fields"] = ["accepted_count"]
        path = self.root / "catalog.json"
        path.write_text(json.dumps(catalog))
        adapter = self.adapter(catalog_path=path)
        result = adapter.execute("SELECT * FROM v_ingestion_freshness")
        self.assertEqual(result.columns, ("accepted_count",))
        self.assertEqual(result.rows, [(24,)])
        with self.assertRaises(QueryAdapterError) as caught:
            adapter.execute("SELECT expected_count FROM v_ingestion_freshness")
        self.assertEqual(caught.exception.code, "query_failure")

    def test_unknown_fixture_scenario_returns_sanitized_adapter_failure(self):
        document = json.loads(self.manifest.read_text())
        document["fixture_evidence"]["scenario"] = "invalid-private-marker"
        self.manifest.write_text(json.dumps(document))
        with self.assertRaises(QueryAdapterError) as caught:
            self.adapter().execute("SELECT * FROM v_ingestion_freshness")
        self.assertEqual(caught.exception.code, "invalid_freshness_manifest")
        self.assertNotIn("invalid-private-marker", str(caught.exception))
        self.assertNotIn("unknown fixture scenario", str(caught.exception))

    def test_row_limit_returns_no_partial_success(self):
        from pipelines.batch.bronze import ingest_pageviews
        from pipelines.batch.silver import normalize_pageviews

        body = gzip.compress(b"en Main_Page 1 10\nfr Main_Page 2 20\n", mtime=0)
        bronze = ingest_pageviews(
            "2024-01-01", "daily", self.root, run_id="row-bronze",
            downloader=lambda _: test_gold_metrics._Response(body),
        )
        silver = normalize_pageviews(bronze, self.root, run_id="row-silver")
        self.manifest = materialize_project_traffic_daily(silver, self.root, run_id="row-gold")
        with self.assertRaises(QueryAdapterError) as caught:
            self.adapter(limits=QueryLimits(max_rows=1)).execute("SELECT project_code FROM v_project_traffic_daily")
        self.assertEqual(caught.exception.code, "row_limit")

    def test_gold_aggregation_and_silver_lineage(self):
        fixture = test_gold_metrics.GoldMetricsTests()
        self.manifest = materialize_project_traffic_daily(fixture._daily_silver(self.root), self.root, run_id="gold")
        result = self.adapter().execute("SELECT SUM(view_count) AS total FROM v_project_traffic_daily")
        self.assertEqual(result.rows, [(4872,)])
        self.assertEqual(result.source_datasets, ("project_traffic_daily", "pageviews_hourly"))
        self.assertEqual([source.dataset_id for source in result.sources], list(result.source_datasets))
        self.assertEqual(result.sources[1].manifest_id, "silver-daily")
        self.assertEqual(result.sources[0].partition_date, "2024-01-01")
