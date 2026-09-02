from __future__ import annotations

import gzip
import hashlib
from http.client import IncompleteRead
import io
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from pipelines.batch.bronze import (
    BronzeIngestionError,
    SourcePartition,
    expected_source_partitions,
    ingest_pageviews,
)


class _Response:
    def __init__(self, body: bytes, *, status: int = 200, headers: dict[str, str] | None = None) -> None:
        self._body = io.BytesIO(body)
        self.status = status
        self.headers = headers or {
            "Content-Length": str(len(body)),
            "ETag": '"fixture-etag"',
            "Last-Modified": "Mon, 01 Jan 2024 01:00:00 GMT",
        }

    def read(self, size: int = -1) -> bytes:
        return self._body.read(size)

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_: object) -> None:
        self._body.close()


class _ReadFailureResponse(_Response):
    def read(self, size: int = -1) -> bytes:
        raise IncompleteRead(b"partial", 10)


class _ResponseOSError(_Response):
    def read(self, size: int = -1) -> bytes:
        raise OSError("response connection reset")


class _WriteFailure:
    def write(self, _: bytes) -> int:
        raise OSError("staging disk full")

    def __enter__(self) -> _WriteFailure:
        return self

    def __exit__(self, *_: object) -> None:
        return None


class BronzePageviewsTest(unittest.TestCase):
    partition_date = "2024-01-01"
    now = staticmethod(lambda: datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC))

    def test_tiny_ingestion_preserves_source_provenance_logical_time_and_run_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            destination = Path(temporary_directory) / "bronze-output"
            result = ingest_pageviews(
                self.partition_date,
                "tiny",
                destination,
                downloader=self._valid_downloader(),
                run_id="tiny-run",
                now=self.now,
            )

            manifest = json.loads(result.read_text(encoding="utf-8"))
            self.assertEqual(manifest["profile"], "tiny")
            self.assertEqual(manifest["run"]["run_id"], "tiny-run")
            self.assertEqual(manifest["run"]["started_at"], "2024-01-02T03:04:05Z")
            self.assertEqual(manifest["partition_date"], self.partition_date)
            self.assertEqual(len(manifest["source_objects"]), 1)
            source = manifest["source_objects"][0]
            self.assertEqual(source["capture_end"], "2024-01-01T01:00:00Z")
            self.assertEqual(source["logical_hour"], "2024-01-01T00:00:00Z")
            self.assertEqual(source["source_content_length"], source["downloaded_byte_count"])
            self.assertEqual(source["source_sha256"], hashlib.sha256(self._valid_gzip()).hexdigest())
            self.assertTrue(source["source_url"].startswith("https://dumps.wikimedia.org/"))
            self.assertEqual(source["source_etag"], '"fixture-etag"')
            self.assertEqual(source["source_last_modified"], "Mon, 01 Jan 2024 01:00:00 GMT")
            self.assertEqual(source["source_content_length"], len(self._valid_gzip()))
            self.assertEqual(source["retrieved_at"], "2024-01-02T03:04:05Z")
            self.assertEqual(
                set(source) & {
                    "source_url",
                    "source_last_modified",
                    "source_etag",
                    "source_content_length",
                    "source_sha256",
                    "retrieved_at",
                },
                {
                    "source_url",
                    "source_last_modified",
                    "source_etag",
                    "source_content_length",
                    "source_sha256",
                    "retrieved_at",
                },
            )
            self.assertNotIn("fixture_sha256", source)
            self.assertNotIn("upstream_source_sha256", source)
            self.assertTrue((destination / source["object_path"]).is_file())

    def test_profiles_select_one_six_and_twenty_four_pinned_source_partitions(self) -> None:
        self.assertEqual(len(expected_source_partitions(self.partition_date, "tiny")), 1)
        self.assertEqual(len(expected_source_partitions(self.partition_date, "demo")), 6)
        daily = expected_source_partitions(self.partition_date, "daily")
        self.assertEqual(len(daily), 24)
        self.assertEqual(daily[-1].capture_end, "2024-01-02T00:00:00Z")
        self.assertTrue(daily[-1].source_url.endswith("pageviews-20240102-000000.gz"))

    def test_demo_and_daily_profiles_publish_every_selected_pinned_partition(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            for profile, expected_count in (("demo", 6), ("daily", 24)):
                with self.subTest(profile=profile):
                    manifest_path = ingest_pageviews(
                        self.partition_date,
                        profile,
                        root / profile,
                        downloader=self._valid_downloader(),
                        run_id=f"{profile}-run",
                        now=self.now,
                    )
                    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                    self.assertEqual(len(manifest["source_objects"]), expected_count)
                    self.assertTrue(all((root / profile / source["object_path"]).is_file() for source in manifest["source_objects"]))

    def test_year_boundary_uses_next_capture_end_but_logical_day_hour_twenty_three(self) -> None:
        partition_date = "2024-12-31"
        daily = expected_source_partitions(partition_date, "daily")
        self.assertEqual(daily[-1].capture_end, "2025-01-01T00:00:00Z")
        self.assertTrue(daily[-1].source_url.endswith("2025/2025-01/pageviews-20250101-000000.gz"))
        with tempfile.TemporaryDirectory() as temporary_directory:
            destination = Path(temporary_directory) / "bronze-output"
            manifest_path = ingest_pageviews(
                partition_date,
                "daily",
                destination,
                downloader=self._valid_downloader(),
                run_id="year-boundary",
                now=self.now,
            )
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            final_source = manifest["source_objects"][-1]
            self.assertEqual(final_source["logical_hour"], "2024-12-31T23:00:00Z")
            self.assertIn("partition_date=2024-12-31/hour=23/", final_source["object_path"])

    def test_missing_malformed_truncated_and_conflicting_inputs_fail_without_publication(self) -> None:
        cases = (
            ("missing", self._valid_downloader(status=404), None),
            ("malformed", self._valid_downloader(body=b"not-gzip"), None),
            ("truncated", self._valid_downloader(body=self._valid_gzip()[:-5]), None),
            (
                "conflicting",
                self._valid_downloader(),
                [
                    SourcePartition("2024-01-01T01:00:00Z", "https://dumps.wikimedia.org/two.gz"),
                ],
            ),
        )
        for name, downloader, source_partitions in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary_directory:
                destination = Path(temporary_directory) / "bronze-output"
                with self.assertRaises(BronzeIngestionError):
                    ingest_pageviews(
                        self.partition_date,
                        "tiny",
                        destination,
                        downloader=downloader,
                        source_partitions=source_partitions,
                        run_id=f"failed-{name}",
                        now=self.now,
                    )
                self._assert_no_publication(destination)

    def test_incorrect_content_length_fails_without_silently_correcting_source_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            destination = Path(temporary_directory) / "bronze-output"
            with self.assertRaises(BronzeIngestionError):
                ingest_pageviews(
                    self.partition_date,
                    "tiny",
                    destination,
                    downloader=self._valid_downloader(headers={
                        "Content-Length": "1",
                        "ETag": '"fixture-etag"',
                        "Last-Modified": "Mon, 01 Jan 2024 01:00:00 GMT",
                    }),
                    run_id="bad-length",
                    now=self.now,
                )
            self._assert_no_publication(destination)

    def test_failure_diagnostic_codes_are_stable(self) -> None:
        cases = (
            ("missing", self._valid_downloader(status=404), None, "source_http_status"),
            ("malformed", self._valid_downloader(body=b"not-gzip"), None, "invalid_gzip_source"),
            ("truncated", self._valid_downloader(body=self._valid_gzip()[:-5]), None, "invalid_gzip_source"),
            (
                "conflicting",
                self._valid_downloader(),
                [
                    SourcePartition("2024-01-01T01:00:00Z", "https://dumps.wikimedia.org/two.gz"),
                ],
                "conflicting_source_partitions",
            ),
            (
                "content-length",
                self._valid_downloader(headers={
                    "Content-Length": "1",
                    "ETag": '"fixture-etag"',
                    "Last-Modified": "Mon, 01 Jan 2024 01:00:00 GMT",
                }),
                None,
                "conflicting_source_metadata",
            ),
        )
        for name, downloader, source_partitions, expected_code in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary_directory:
                with self.assertRaises(BronzeIngestionError) as raised:
                    ingest_pageviews(
                        self.partition_date,
                        "tiny",
                        Path(temporary_directory) / "bronze-output",
                        downloader=downloader,
                        source_partitions=source_partitions,
                        run_id=f"code-{name}",
                        now=self.now,
                    )
                self.assertEqual(raised.exception.code, expected_code)

    def test_incomplete_http_read_has_stable_diagnostic_and_no_publication(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            destination = Path(temporary_directory) / "bronze-output"
            with self.assertRaises(BronzeIngestionError) as raised:
                ingest_pageviews(
                    self.partition_date,
                    "tiny",
                    destination,
                    downloader=lambda _: _ReadFailureResponse(self._valid_gzip()),
                    run_id="read-failure",
                    now=self.now,
                )
            self.assertEqual(raised.exception.code, "source_read_failure")
            self._assert_no_publication(destination)

    def test_response_os_error_has_source_read_diagnostic_and_no_publication(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            destination = Path(temporary_directory) / "bronze-output"
            with self.assertRaises(BronzeIngestionError) as raised:
                ingest_pageviews(
                    self.partition_date,
                    "tiny",
                    destination,
                    downloader=lambda _: _ResponseOSError(self._valid_gzip()),
                    run_id="response-os-error",
                    now=self.now,
                )
            self.assertEqual(raised.exception.code, "source_read_failure")
            self._assert_no_publication(destination)

    def test_staging_open_and_write_errors_have_distinct_diagnostic_and_no_publication(self) -> None:
        cases = (
            ("open", mock.patch("pipelines.batch.bronze.Path.open", side_effect=OSError("staging permission denied"))),
            ("write", mock.patch("pipelines.batch.bronze.Path.open", return_value=_WriteFailure())),
        )
        for name, patcher in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary_directory, patcher:
                destination = Path(temporary_directory) / "bronze-output"
                with self.assertRaises(BronzeIngestionError) as raised:
                    ingest_pageviews(
                        self.partition_date,
                        "tiny",
                        destination,
                        downloader=self._valid_downloader(),
                        run_id=f"staging-{name}",
                        now=self.now,
                    )
                self.assertEqual(raised.exception.code, "staging_write_failure")
                self._assert_no_publication(destination)

    def test_repeated_and_concurrent_publication_never_overwrite_an_existing_run(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            destination = Path(temporary_directory) / "bronze-output"

            def publish() -> str:
                try:
                    ingest_pageviews(
                        self.partition_date,
                        "tiny",
                        destination,
                        downloader=self._valid_downloader(),
                        run_id="same-run",
                        now=self.now,
                    )
                except BronzeIngestionError:
                    return "rejected"
                return "published"

            with ThreadPoolExecutor(max_workers=2) as executor:
                outcomes = list(executor.map(lambda _: publish(), range(2)))
            self.assertEqual(sorted(outcomes), ["published", "rejected"])
            with self.assertRaises(BronzeIngestionError):
                publish_result = ingest_pageviews(
                    self.partition_date,
                    "tiny",
                    destination,
                    downloader=self._valid_downloader(),
                    run_id="same-run",
                    now=self.now,
                )
                self.assertIsNone(publish_result)

    def test_symlink_containment_and_link_failure_leave_no_manifest_or_partial_run(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            real_destination = root / "real"
            real_destination.mkdir()
            linked_destination = root / "linked"
            linked_destination.symlink_to(real_destination, target_is_directory=True)
            with self.assertRaises(BronzeIngestionError):
                ingest_pageviews(
                    self.partition_date,
                    "tiny",
                    linked_destination,
                    downloader=self._valid_downloader(),
                    run_id="linked",
                    now=self.now,
                )
            self._assert_no_publication(real_destination)

            destination = root / "bronze-output"
            with mock.patch("pipelines.batch.bronze.os.link", side_effect=OSError("disk full")):
                with self.assertRaises(BronzeIngestionError):
                    ingest_pageviews(
                        self.partition_date,
                        "tiny",
                        destination,
                        downloader=self._valid_downloader(),
                        run_id="link-failure",
                        now=self.now,
                    )
            self._assert_no_publication(destination)

    def test_manifest_link_failure_cleans_bronze_runs_after_object_publication(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            destination = Path(temporary_directory) / "bronze-output"
            real_link = os.link

            def fail_manifest_link(source: Path, target: Path) -> None:
                if Path(source).name == "manifest.json":
                    raise OSError("manifest disk failure")
                real_link(source, target)

            with mock.patch("pipelines.batch.bronze.os.link", side_effect=fail_manifest_link):
                with self.assertRaises(BronzeIngestionError) as raised:
                    ingest_pageviews(
                        self.partition_date,
                        "tiny",
                        destination,
                        downloader=self._valid_downloader(),
                        run_id="manifest-link-failure",
                        now=self.now,
                    )
            self.assertEqual(raised.exception.code, "publication_failure")
            self.assertFalse(
                (destination / "manifests" / "pageviews_hourly" / "partition_date=2024-01-01" / "manifest-link-failure.json").exists()
            )
            self.assertEqual(list((destination / "bronze").rglob("run_id=*")), [])
            self.assertEqual(list((destination / "bronze").rglob("*.gz")), [])

    @staticmethod
    def _valid_gzip() -> bytes:
        return gzip.compress(b"en LakeOps_Agent 101 4097\n", mtime=0)

    def _valid_downloader(
        self,
        *,
        body: bytes | None = None,
        status: int = 200,
        headers: dict[str, str] | None = None,
    ):
        response_body = self._valid_gzip() if body is None else body

        def downloader(_: str) -> _Response:
            return _Response(response_body, status=status, headers=headers)

        return downloader

    def _assert_no_publication(self, destination: Path) -> None:
        self.assertFalse((destination / "manifests").exists())
        bronze = destination / "bronze"
        if bronze.exists():
            self.assertEqual(list(bronze.rglob("*.gz")), [])
            self.assertEqual(list(bronze.rglob("run_id=*")), [])


if __name__ == "__main__":
    unittest.main()
