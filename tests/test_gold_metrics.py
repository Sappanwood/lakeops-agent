"""Governed Gold metric and DuckDB view tests."""

from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

from pipelines.batch.bronze import ingest_pageviews
from pipelines.batch.gold import (
    GovernedQueryError,
    GoldMaterializationError,
    materialize_fixture_ingestion_freshness,
    materialize_project_traffic_daily,
    open_governed_query,
)
from pipelines.batch.silver import normalize_pageviews


class _Response:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.offset = 0
        self.status = 200
        self.headers = {
            "Content-Length": str(len(body)),
            "ETag": '"gold-fixture"',
            "Last-Modified": "Mon, 01 Jan 2024 01:00:00 GMT",
        }

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self.body) - self.offset
        result = self.body[self.offset : self.offset + size]
        self.offset += len(result)
        return result

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class GoldMetricsTests(unittest.TestCase):
    now = staticmethod(lambda: datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC))

    def test_catalog_kpi_formula_and_unit_drift_fail_closed(self) -> None:
        catalog_source = Path(__file__).resolve().parents[1] / "data" / "catalog" / "catalog.json"
        for field, value in (("formula", "avg(pageviews_hourly.view_count)"), ("unit", "bananas")):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as temporary_directory:
                root = Path(temporary_directory)
                silver_manifest = self._daily_silver(root)
                catalog = json.loads(catalog_source.read_text(encoding="utf-8"))
                project_views = next(kpi for kpi in catalog["kpis"] if kpi["id"] == "kpi.project_daily_views")
                project_views[field] = value
                catalog_path = root / "catalog.json"
                catalog_path.write_text(json.dumps(catalog), encoding="utf-8")

                with self.assertRaises(GoldMaterializationError) as raised:
                    materialize_project_traffic_daily(
                        silver_manifest,
                        root,
                        run_id=f"gold-kpi-{field}",
                        catalog_path=catalog_path,
                        now=self.now,
                    )

                self.assertEqual(raised.exception.code, "catalog_contract_failure")
                self.assertFalse((root / "gold").exists())

    def test_daily_gold_metrics_and_catalog_governed_views_are_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            silver_manifest = self._daily_silver(root)

            manifest_path = materialize_project_traffic_daily(
                silver_manifest, root, run_id="gold-daily", now=self.now
            )

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["schema"], "lakeops/gold-project-traffic-manifest@1")
            self.assertEqual(manifest["status"], "accepted")
            self.assertEqual(manifest["kpi"]["id"], "kpi.project_daily_views")
            self.assertEqual(manifest["kpi"]["unit"], "views")
            self.assertTrue(manifest["freshness"]["is_complete"])
            self.assertEqual(manifest["input_manifest_ids"], ["silver-daily"])
            self.assertEqual(manifest["processing"]["input_hour_count"], 24)
            self.assertEqual(manifest["processing"]["duckdb_buffer_memory_limit"], "256MB")
            self.assertEqual(manifest["processing"]["unique_page_count_strategy"], "external_sort_adjacent_keys")
            self.assertNotIn(str(root), json.dumps(manifest))
            with self.assertRaises(GoldMaterializationError) as repeated:
                materialize_project_traffic_daily(silver_manifest, root, run_id="gold-daily", now=self.now)
            self.assertEqual(repeated.exception.code, "publication_conflict")

            with open_governed_query(manifest_path, root) as session:
                self.assertEqual(
                    session.query(
                        "v_project_traffic_daily",
                        ["project_code", "view_count", "response_bytes", "unique_page_count", "input_hour_count", "is_complete"],
                    ),
                    [("en", 4872, 110616, 2, 24, True)],
                )
                for unavailable in ("v_page_activity_hourly", "v_page_traffic_activity", "v_ingestion_freshness", "v_pipeline_runs"):
                    with self.subTest(unavailable=unavailable), self.assertRaises(GovernedQueryError) as unavailable_error:
                        session.query(unavailable, ["project_code"] if unavailable != "v_pipeline_runs" else ["run_id"])
                    self.assertEqual(unavailable_error.exception.code, "view_unavailable")

    def test_complete_partition_marks_sparse_project_complete(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)

            def downloader(url: str) -> _Response:
                body = b"en Main_Page 1 10\n"
                if url.endswith("010000.gz"):
                    body += b"rare Sparse_Page 2 20\n"
                return _Response(gzip.compress(body, mtime=0))

            bronze = ingest_pageviews(
                "2024-01-01",
                "daily",
                root,
                downloader=downloader,
                run_id="sparse-bronze",
                now=self.now,
            )
            silver = normalize_pageviews(bronze, root, run_id="sparse-silver", now=self.now)
            gold = materialize_project_traffic_daily(silver, root, run_id="sparse-gold", now=self.now)

            with open_governed_query(gold, root) as session:
                rows = session.query(
                    "v_project_traffic_daily",
                    ["project_code", "input_hour_count", "is_complete"],
                )
            self.assertIn(("rare", 24, True), rows)

    def test_unaccepted_incomplete_or_noncanonical_inputs_never_publish_gold_or_open_query_surface(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            bronze = self._ingest(root, "tiny", "tiny-bronze")
            silver = normalize_pageviews(bronze, root, run_id="tiny-silver", now=self.now)
            with self.assertRaises(GoldMaterializationError) as raised:
                materialize_project_traffic_daily(silver, root, run_id="gold-incomplete", now=self.now)
            self.assertEqual(raised.exception.code, "incomplete_silver_inputs")
            self.assertFalse((root / "gold").exists())

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            silver = self._daily_silver(root)
            document = json.loads(silver.read_text(encoding="utf-8"))
            document["output_objects"][0]["object_path"] = "/tmp/attacker.parquet"
            silver.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaises(GoldMaterializationError) as raised:
                materialize_project_traffic_daily(silver, root, run_id="gold-path", now=self.now)
            self.assertEqual(raised.exception.code, "broken_silver_join")
            self.assertFalse((root / "gold").exists())

    def test_query_api_rejects_undocumented_views_fields_and_external_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            manifest = materialize_project_traffic_daily(
                self._daily_silver(root), root, run_id="gold-query", now=self.now
            )
            with open_governed_query(manifest, root) as session:
                with self.assertRaises(GovernedQueryError) as view_error:
                    session.query("pageviews_hourly", ["project_code"])
                self.assertEqual(view_error.exception.code, "unknown_view")
                with self.assertRaises(GovernedQueryError) as field_error:
                    session.query("v_project_traffic_daily", ["source_object"])
                self.assertEqual(field_error.exception.code, "unknown_field")
                with self.assertRaises(GovernedQueryError) as injection_error:
                    session.query("v_project_traffic_daily; SELECT * FROM read_parquet('/tmp/x')", ["project_code"])
                self.assertEqual(injection_error.exception.code, "unknown_view")
            with self.assertRaises(GovernedQueryError) as path_error:
                open_governed_query(root / "outside.json", root)
            self.assertEqual(path_error.exception.code, "unsafe_input")

    def test_concurrent_gold_publication_and_query_sessions_keep_owned_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            silver = self._daily_silver(root)

            def publish() -> str:
                try:
                    return str(materialize_project_traffic_daily(silver, root, run_id="gold-concurrent", now=self.now))
                except GoldMaterializationError as error:
                    return error.code

            with ThreadPoolExecutor(max_workers=2) as executor:
                outcomes = sorted(executor.map(lambda _: publish(), range(2)))
            self.assertEqual(outcomes.count("publication_conflict"), 1)
            published = root / "manifests" / "project_traffic_daily" / "partition_date=2024-01-01" / "gold-concurrent.json"
            self.assertTrue(published.is_file())
            manifest = json.loads(published.read_text(encoding="utf-8"))
            self.assertTrue((root / manifest["output_objects"][0]["object_path"]).is_file())

            with ThreadPoolExecutor(max_workers=2) as executor:
                sessions = list(executor.map(lambda _: open_governed_query(published, root), range(2)))
            self.assertTrue(all(session.query("v_project_traffic_daily", ["project_code"]) == [("en",)] for session in sessions))
            for session in sessions:
                session.close()
            self.assertEqual(list((root / ".duckdb-query").iterdir()), [])

            session = open_governed_query(published, root)
            with mock.patch("pipelines.batch.gold._register_catalog_views", side_effect=GoldMaterializationError("test_failure", "forced")):
                with self.assertRaises(GoldMaterializationError):
                    open_governed_query(published, root)
            self.assertEqual(session.query("v_project_traffic_daily", ["project_code"]), [("en",)])
            session.close()
            self.assertEqual(list((root / ".duckdb-query").iterdir()), [])

    def test_committed_fixture_freshness_distinguishes_missing_hour_from_complete_day(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            missing = materialize_fixture_ingestion_freshness("missing_hour_day", root, run_id="fresh-missing", now=self.now)
            complete = materialize_fixture_ingestion_freshness("complete_day", root, run_id="fresh-complete", now=self.now)
            with open_governed_query(missing, root) as session:
                self.assertEqual(
                    session.query("v_ingestion_freshness", ["source_id", "expected_count", "accepted_count", "freshness_status"]),
                    [("wikimedia_pageviews", 24, 23, "missing")],
                )
                with self.assertRaises(GovernedQueryError) as traffic_error:
                    session.query("v_project_traffic_daily", ["project_code"])
                self.assertEqual(traffic_error.exception.code, "view_unavailable")
            with open_governed_query(complete, root) as session:
                self.assertEqual(
                    session.query("v_ingestion_freshness", ["expected_count", "accepted_count", "freshness_status"]),
                    [(24, 24, "complete")],
                )

    def test_query_rejects_silver_lineage_and_run_count_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            silver = self._daily_silver(root)
            gold = materialize_project_traffic_daily(silver, root, run_id="gold-lineage", now=self.now)
            document = json.loads(silver.read_text(encoding="utf-8"))
            document["run"]["row_count"] += 1
            silver.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaises(GovernedQueryError) as drift:
                open_governed_query(gold, root)
            self.assertEqual(drift.exception.code, "invalid_silver_manifest")

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            silver = self._daily_silver(root)
            gold = materialize_project_traffic_daily(silver, root, run_id="gold-byte-lineage", now=self.now)
            silver.write_text(silver.read_text(encoding="utf-8") + "\n", encoding="utf-8")
            with self.assertRaises(GovernedQueryError) as drift:
                open_governed_query(gold, root)
            self.assertEqual(drift.exception.code, "broken_gold_join")

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            gold = materialize_project_traffic_daily(self._daily_silver(root), root, run_id="gold-run-count", now=self.now)
            document = json.loads(gold.read_text(encoding="utf-8"))
            document["run"]["row_count"] += 1
            gold.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaises(GovernedQueryError) as drift:
                open_governed_query(gold, root)
            self.assertEqual(drift.exception.code, "invalid_gold_manifest")

    def _daily_silver(self, root: Path) -> Path:
        return normalize_pageviews(self._ingest(root, "daily", "daily-bronze"), root, run_id="silver-daily", now=self.now)

    def _ingest(self, root: Path, profile: str, run_id: str) -> Path:
        body = gzip.compress(b"en LakeOps_Agent 101 4097\nen Main_Page 102 512\n", mtime=0)
        return ingest_pageviews(
            "2024-01-01",
            profile,
            root,
            downloader=lambda _: _Response(body),
            run_id=run_id,
            now=self.now,
        )


if __name__ == "__main__":
    unittest.main()
