from __future__ import annotations

import gzip
import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest import mock

import duckdb

from pipelines.batch.bronze import ingest_pageviews
import pipelines.batch.silver as silver
from pipelines.batch.silver import PHYSICAL_TYPES, SilverNormalizationError, normalize_pageviews


class _Response:
    def __init__(self, body: bytes) -> None:
        self._body = body
        self.status = 200
        self.headers = {
            "Content-Length": str(len(body)),
            "ETag": '"fixture-etag"',
            "Last-Modified": "Mon, 01 Jan 2024 01:00:00 GMT",
        }
        self._offset = 0

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self._body) - self._offset
        result = self._body[self._offset : self._offset + size]
        self._offset += len(result)
        return result

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_: object) -> None:
        return None


class SilverPageviewsTest(unittest.TestCase):
    now = staticmethod(lambda: datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC))

    def test_wikimedia_canonical_dbkey_preserves_literal_percent(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            bronze = self._ingest(
                root,
                "2024-01-01",
                "tiny",
                "literal-percent-bronze",
                body=gzip.compress(b"en 100%_Real 7 70\n", mtime=0),
            )
            silver = normalize_pageviews(bronze, root, run_id="literal-percent-silver", now=self.now)
            manifest = json.loads(silver.read_text(encoding="utf-8"))
            output = root / manifest["output_objects"][0]["object_path"]
            self.assertEqual(duckdb.sql(f"SELECT page_title FROM read_parquet('{output}')").fetchall(), [("100%_Real",)])

    def test_accepted_canonical_bronze_manifest_publishes_typed_parquet(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            bronze_manifest = self._ingest(root, "2024-01-01", "tiny", "bronze-ok")

            result = normalize_pageviews(bronze_manifest, root, run_id="silver-ok", now=self.now)

            manifest = json.loads(result.read_text(encoding="utf-8"))
            self.assertEqual(manifest["schema"], "lakeops/silver-pageviews-manifest@1")
            self.assertEqual(manifest["status"], "accepted")
            self.assertEqual(manifest["physical_schema"], PHYSICAL_TYPES)
            self.assertGreaterEqual(manifest["processing"]["duplicate_aggregation"]["peak_temp_directory_bytes"], 0)
            self.assertEqual(
                manifest["input_manifest"]["path"],
                bronze_manifest.relative_to(root).as_posix(),
            )
            self.assertNotIn(str(root), json.dumps(manifest))
            output = root / manifest["output_objects"][0]["object_path"]
            self.assertEqual(output.suffix, ".parquet")
            connection = duckdb.connect()
            try:
                description = connection.execute(
                    "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning = false)", [str(output)]
                ).fetchall()
                self.assertEqual({name: data_type.upper() for name, data_type, *_ in description}, PHYSICAL_TYPES)
                rows = connection.execute(
                    "SELECT project_code, page_title, view_count, response_bytes, "
                    "strftime(window_end, '%Y-%m-%dT%H:%M:%SZ'), partition_date::VARCHAR, hour, source_object "
                    "FROM read_parquet(?, hive_partitioning = false) ORDER BY page_title",
                    [str(output)],
                ).fetchall()
            finally:
                connection.close()
            self.assertEqual(
                rows,
                [
                    ("en", "LakeOps_Agent", 101, 4097, "2024-01-01T01:00:00Z", "2024-01-01", 0, manifest["output_objects"][0]["source_object"]),
                    ("en", "Main_Page", 102, 512, "2024-01-01T01:00:00Z", "2024-01-01", 0, manifest["output_objects"][0]["source_object"]),
                ],
            )

    def test_arbitrary_manifest_path_is_rejected_and_only_quarantined(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            bronze_manifest = self._ingest(root, "2024-01-01", "tiny", "arbitrary")
            arbitrary = root / "copied" / "input.json"
            arbitrary.parent.mkdir()
            arbitrary.write_bytes(bronze_manifest.read_bytes())

            self._assert_rejected(arbitrary, root, "silver-arbitrary", "invalid_bronze_manifest")

    def test_invalid_manifest_schema_is_quarantined_without_silver_publication(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            bronze_manifest = self._ingest(root, "2024-01-01", "tiny", "invalid-schema")
            self._mutate_manifest(bronze_manifest, lambda document: document.__setitem__("schema", "lakeops/bronze-pageviews-manifest@0"))

            self._assert_rejected(bronze_manifest, root, "silver-invalid-schema", "invalid_bronze_manifest")

    def test_profile_object_count_and_continuity_fail_closed(self) -> None:
        for mutation in ("short", "long", "noncontinuous", "boolean-input-count"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary_directory:
                root = Path(temporary_directory)
                bronze_manifest = self._ingest(root, "2024-01-01", "daily", f"daily-{mutation}")
                document = json.loads(bronze_manifest.read_text(encoding="utf-8"))
                if mutation == "short":
                    document["source_objects"].pop()
                    document["run"]["input_object_count"] = 23
                elif mutation == "long":
                    document["source_objects"].append(dict(document["source_objects"][0]))
                    document["run"]["input_object_count"] = 25
                elif mutation == "noncontinuous":
                    document["source_objects"][1]["logical_hour"] = "2024-01-01T07:00:00Z"
                    document["source_objects"][1]["capture_end"] = "2024-01-01T08:00:00Z"
                else:
                    document["run"]["input_object_count"] = True
                bronze_manifest.write_text(json.dumps(document), encoding="utf-8")

                expected_code = "broken_bronze_join" if mutation == "noncontinuous" else "invalid_bronze_manifest"
                self._assert_rejected(bronze_manifest, root, f"silver-daily-{mutation}", expected_code)

    def test_source_identity_runtime_provenance_and_capture_boundary_fail_closed(self) -> None:
        mutations = {
            "source-url": lambda source: source.__setitem__("source_url", "https://example.invalid/not-wikimedia.gz"),
            "last-modified": lambda source: source.__setitem__("source_last_modified", ""),
            "etag": lambda source: source.__setitem__("source_etag", 5),
            "content-length": lambda source: source.__setitem__("source_content_length", "5"),
            "boolean-content-length": lambda source: source.__setitem__("source_content_length", True),
            "download-count": lambda source: source.__setitem__("downloaded_byte_count", 1),
            "boolean-download-count": lambda source: source.__setitem__("downloaded_byte_count", True),
            "boolean-record-count": lambda source: source.__setitem__("record_count", True),
            "retrieved-at": lambda source: source.__setitem__("retrieved_at", "not-a-timestamp"),
            "capture-boundary": lambda source: source.__setitem__("capture_end", "2024-01-01T01:01:00Z"),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary_directory:
                root = Path(temporary_directory)
                bronze_manifest = self._ingest(root, "2024-01-01", "tiny", f"provenance-{name}")
                document = json.loads(bronze_manifest.read_text(encoding="utf-8"))
                mutate(document["source_objects"][0])
                bronze_manifest.write_text(json.dumps(document), encoding="utf-8")

                expected_code = "broken_bronze_join" if name == "source-url" else "invalid_bronze_manifest"
                self._assert_rejected(bronze_manifest, root, f"silver-provenance-{name}", expected_code)

    def test_duplicate_primary_key_diagnostic_is_lexicographically_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            duplicate_manifest = self._ingest(
                root,
                "2024-01-01",
                "tiny",
                "duplicate-key",
                body=gzip.compress(
                    b"en Zoo 101 4097\nen Alpha 102 512\nen Zoo 103 513\nen Alpha 104 514\n", mtime=0
                ),
            )
            with self.assertRaises(SilverNormalizationError) as raised:
                normalize_pageviews(duplicate_manifest, root, run_id="silver-duplicate-key", now=self.now)
            self.assertEqual(raised.exception.code, "duplicate_primary_key")
            self.assertIn("('en', 'Alpha', '2024-01-01T01:00:00Z')", raised.exception.detail)
            self._assert_rejection_evidence(duplicate_manifest, root, "silver-duplicate-key", "duplicate_primary_key")

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            bronze_manifest = self._ingest(root, "2024-01-01", "tiny", "broken-join")
            document = json.loads(bronze_manifest.read_text(encoding="utf-8"))
            source_path = root / document["source_objects"][0]["object_path"]
            before = source_path.read_bytes()
            document["source_objects"][0]["source_sha256"] = "0" * 64
            bronze_manifest.write_text(json.dumps(document), encoding="utf-8")
            self._assert_rejected(bronze_manifest, root, "silver-broken-join", "broken_bronze_join")
            self.assertEqual(source_path.read_bytes(), before)

    def test_invalid_source_schema_and_quality_failure_are_quarantined(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            bronze_manifest = self._ingest(root, "2024-01-01", "tiny", "invalid-row")
            document = json.loads(bronze_manifest.read_text(encoding="utf-8"))
            source_path = root / document["source_objects"][0]["object_path"]
            invalid_body = gzip.compress(b"en LakeOps_Agent 101\n", mtime=0)
            source_path.write_bytes(invalid_body)
            document["source_objects"][0]["source_sha256"] = hashlib.sha256(invalid_body).hexdigest()
            document["source_objects"][0]["source_content_length"] = len(invalid_body)
            document["source_objects"][0]["downloaded_byte_count"] = len(invalid_body)
            bronze_manifest.write_text(json.dumps(document), encoding="utf-8")
            self._assert_rejected(bronze_manifest, root, "silver-invalid-row", "invalid_source_schema")

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            quality_manifest = self._ingest(
                root,
                "2024-01-01",
                "tiny",
                "quality-failure",
                body=gzip.compress(b"en LakeOps_Agent 0 4097\n", mtime=0),
            )
            self._assert_rejected(quality_manifest, root, "silver-quality-failure", "invalid_view_count")

    def test_retry_preserves_existing_rejection_without_masking_current_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            bronze_manifest = self._ingest(
                root,
                "2024-01-01",
                "tiny",
                "retry-failure",
                body=gzip.compress(b"en Broken 0 10\n", mtime=0),
            )

            with self.assertRaises(SilverNormalizationError) as first:
                normalize_pageviews(bronze_manifest, root, run_id="silver-retry-failure", now=self.now)
            self.assertEqual(first.exception.code, "invalid_view_count")
            rejection = (
                root
                / "quarantine"
                / "pageviews_hourly"
                / "partition_date=2024-01-01"
                / "run_id=silver-retry-failure"
                / "rejection.json"
            )
            before = rejection.read_bytes()

            with self.assertRaises(SilverNormalizationError) as retry:
                normalize_pageviews(bronze_manifest, root, run_id="silver-retry-failure", now=self.now)

            self.assertEqual(retry.exception.code, "invalid_view_count")
            self.assertEqual(rejection.read_bytes(), before)

    def test_year_boundary_uses_logical_partition_not_capture_end_date(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            bronze_manifest = self._ingest(root, "2024-12-31", "daily", "year-boundary")
            result = normalize_pageviews(bronze_manifest, root, run_id="silver-boundary", now=self.now)
            manifest = json.loads(result.read_text(encoding="utf-8"))
            final_output = root / manifest["output_objects"][-1]["object_path"]
            rows = duckdb.sql(
                "SELECT partition_date::VARCHAR, strftime(window_end, '%Y-%m-%dT%H:%M:%SZ'), hour "
                f"FROM read_parquet('{str(final_output).replace("'", "''")}', hive_partitioning = false)"
            ).fetchall()
            self.assertEqual(rows, [("2024-12-31", "2025-01-01T00:00:00Z", 23), ("2024-12-31", "2025-01-01T00:00:00Z", 23)])

    def test_subprocess_stress_spills_with_run_contained_temp_paths_and_bounded_rss(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            child_cwd = root / "child-cwd"
            child_cwd.mkdir()
            script = root / "stress.py"
            script.write_text(
                textwrap.dedent(
                    """
                    import gzip
                    import json
                    import resource
                    import sys
                    from datetime import UTC, datetime
                    from pathlib import Path

                    from pipelines.batch.bronze import ingest_pageviews
                    import pipelines.batch.silver as silver

                    class Response:
                        def __init__(self, body):
                            self.body = body
                            self.offset = 0
                            self.status = 200
                            self.headers = {
                                'Content-Length': str(len(body)),
                                'ETag': '\"fixture-etag\"',
                                'Last-Modified': 'Mon, 01 Jan 2024 01:00:00 GMT',
                            }
                        def read(self, size=-1):
                            if size < 0:
                                size = len(self.body) - self.offset
                            value = self.body[self.offset:self.offset + size]
                            self.offset += len(value)
                            return value
                        def __enter__(self): return self
                        def __exit__(self, *args): return None

                    root = Path(sys.argv[1])
                    body = gzip.compress(
                        ''.join(f'en Page_{number:06d} 1 1\\n' for number in range(500_000)).encode(),
                        mtime=0,
                    )
                    now = lambda: datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
                    bronze = ingest_pageviews(
                        '2024-01-01', 'tiny', root,
                        downloader=lambda _: Response(body), run_id='stress-bronze', now=now,
                    )
                    configured = []
                    original = silver._configure_duckdb
                    def capture(connection, temp_directory):
                        configured.append(str(temp_directory))
                        original(connection, temp_directory)
                    silver._configure_duckdb = capture
                    silver.DUCKDB_MEMORY_LIMIT = '64MB'
                    result = silver.normalize_pageviews(bronze, root, run_id='stress-silver', now=now)
                    manifest = json.loads(result.read_text())
                    print(json.dumps({
                        'peak_temp_directory_bytes': manifest['processing']['duplicate_aggregation']['peak_temp_directory_bytes'],
                        'rss_kib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                        'temp_directories': configured,
                        'cwd_tmp_exists': Path('.tmp').exists(),
                    }))
                    """
                ),
                encoding="utf-8",
            )
            environment = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1])}
            completed = subprocess.run(
                [sys.executable, str(script), str(root)],
                cwd=child_cwd,
                env=environment,
                text=True,
                capture_output=True,
                timeout=120,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            result = json.loads(completed.stdout)
            self.assertGreater(result["peak_temp_directory_bytes"], 0)
            self.assertGreater(result["rss_kib"], 0)
            self.assertLess(result["rss_kib"], 1024 * 1024)
            configured_temp_directories = [Path(value) for value in result["temp_directories"]]
            self.assertEqual(len(configured_temp_directories), 2)
            self.assertTrue(all(path.is_relative_to(root) for path in configured_temp_directories))
            self.assertTrue(all(path.name.startswith("duckdb-") for path in configured_temp_directories))
            self.assertFalse(result["cwd_tmp_exists"])

    def test_repeated_concurrent_and_manifest_link_failure_never_publish_partial_output(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            bronze_manifest = self._ingest(root, "2024-01-01", "tiny", "concurrent")

            def publish() -> str:
                try:
                    normalize_pageviews(bronze_manifest, root, run_id="silver-concurrent", now=self.now)
                except SilverNormalizationError:
                    return "rejected"
                return "published"

            with ThreadPoolExecutor(max_workers=2) as executor:
                self.assertEqual(sorted(executor.map(lambda _: publish(), range(2))), ["published", "rejected"])
            with self.assertRaises(SilverNormalizationError) as raised:
                normalize_pageviews(bronze_manifest, root, run_id="silver-concurrent", now=self.now)
            self.assertEqual(raised.exception.code, "publication_conflict")

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            bronze_manifest = self._ingest(root, "2024-01-01", "tiny", "link-failure")
            real_link = os.link

            def fail_manifest_link(source: Path, target: Path) -> None:
                if Path(source).name == "manifest.json":
                    raise OSError("manifest disk failure")
                real_link(source, target)

            with mock.patch("pipelines.batch.silver.os.link", side_effect=fail_manifest_link):
                with self.assertRaises(SilverNormalizationError) as raised:
                    normalize_pageviews(bronze_manifest, root, run_id="silver-link-failure", now=self.now)
            self.assertEqual(raised.exception.code, "publication_failure")
            self.assertEqual(list((root / "silver").rglob("*.parquet")), [])
            self.assertFalse((root / "manifests" / "pageviews_hourly" / "partition_date=2024-01-01" / "silver-link-failure.json").exists())

    def test_symlink_destination_is_rejected_before_silver_publication(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            real_root = root / "real"
            real_root.mkdir()
            bronze_manifest = self._ingest(real_root, "2024-01-01", "tiny", "linked")
            linked_root = root / "linked"
            linked_root.symlink_to(real_root, target_is_directory=True)
            with self.assertRaises(SilverNormalizationError) as raised:
                normalize_pageviews(linked_root / bronze_manifest.relative_to(real_root), linked_root, run_id="silver-linked", now=self.now)
            self.assertEqual(raised.exception.code, "unsafe_destination")
            self.assertFalse((real_root / "silver").exists())

    def _assert_rejected(self, bronze_manifest: Path, root: Path, run_id: str, code: str) -> None:
        with self.assertRaises(SilverNormalizationError) as raised:
            normalize_pageviews(bronze_manifest, root, run_id=run_id, now=self.now)
        self.assertEqual(raised.exception.code, code)
        self._assert_rejection_evidence(bronze_manifest, root, run_id, code)
        self.assertFalse((root / "silver" / "pageviews_hourly").exists())

    def _assert_rejection_evidence(self, bronze_manifest: Path, root: Path, run_id: str, code: str) -> None:
        evidence = json.loads(
            (
                root
                / "quarantine"
                / "pageviews_hourly"
                / "partition_date=2024-01-01"
                / f"run_id={run_id}"
                / "rejection.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(evidence["status"], "rejected")
        self.assertEqual(evidence["error_code"], code)
        self.assertEqual(evidence["input_manifest"]["path"], bronze_manifest.relative_to(root).as_posix())
        self.assertNotIn(str(root), json.dumps(evidence))

    @staticmethod
    def _mutate_manifest(path: Path, mutate) -> None:
        document = json.loads(path.read_text(encoding="utf-8"))
        mutate(document)
        path.write_text(json.dumps(document), encoding="utf-8")

    def _ingest(self, root: Path, partition_date: str, profile: str, run_id: str, *, body: bytes | None = None) -> Path:
        source_body = body or gzip.compress(b"en LakeOps_Agent 101 4097\nen Main_Page 102 512\n", mtime=0)

        def downloader(_: str) -> _Response:
            return _Response(source_body)

        return ingest_pageviews(partition_date, profile, root, downloader=downloader, run_id=run_id, now=self.now)


if __name__ == "__main__":
    unittest.main()
