"""End-to-end local batch publication tests."""

from __future__ import annotations

import contextlib
import gzip
import hashlib
import io
import json
import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

from pipelines.batch.local import BatchPipelineError, main, run_batch_pipeline


class _Response:
    def __init__(self, body: bytes, source_url: str) -> None:
        self._body = body
        self._offset = 0
        self.status = 200
        self.headers = {
            "Content-Length": str(len(body)),
            "ETag": f'"{hashlib.sha256(source_url.encode("ascii")).hexdigest()[:16]}"',
            "Last-Modified": "Mon, 01 Jan 2024 01:00:00 GMT",
        }

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self._body) - self._offset
        result = self._body[self._offset : self._offset + size]
        self._offset += len(result)
        return result

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class BatchPipelineTests(unittest.TestCase):
    now = staticmethod(lambda: datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC))

    @staticmethod
    def downloader(source_url: str) -> _Response:
        token = int(hashlib.sha256(source_url.encode("ascii")).hexdigest()[:4], 16)
        source = (
            f"en Main_Page {token + 1} {token + 101}\n"
            f"commons.m LakeOps_Logo {token + 2} {token + 202}\n"
        ).encode("utf-8")
        return _Response(gzip.compress(source, mtime=0), source_url)

    @staticmethod
    def _read(path: Path) -> dict[str, object]:
        return json.loads(path.read_text(encoding="utf-8"))

    def test_success_publishes_one_authoritative_manifest_after_all_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            manifest_path = run_batch_pipeline(
                "2024-01-01",
                root,
                run_id="batch-success",
                downloader=self.downloader,
                now=self.now,
            )

            manifest = self._read(manifest_path)
            self.assertEqual(manifest["schema"], "lakeops/batch-pipeline-manifest@1")
            self.assertEqual(manifest["status"], "accepted")
            self.assertEqual(set(manifest["stage_manifests"]), {"bronze", "silver", "gold"})
            self.assertEqual(manifest["run"]["input_object_count"], 24)
            self.assertEqual(manifest["run"]["silver_object_count"], 24)
            self.assertEqual(manifest["run"]["gold_object_count"], 1)
            self.assertNotIn(str(root), json.dumps(manifest))
            for evidence in [*manifest["stage_manifests"].values(), *manifest["output_objects"]]:
                published = root / evidence["path"]
                self.assertTrue(published.is_file())
                self.assertFalse(published.is_symlink())
                self.assertEqual(hashlib.sha256(published.read_bytes()).hexdigest(), evidence["sha256"])

    def test_validation_failure_cannot_replace_last_accepted_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            accepted = run_batch_pipeline(
                "2024-01-01", root, run_id="batch-accepted", downloader=self.downloader, now=self.now
            )
            accepted_bytes = accepted.read_bytes()

            def invalid_downloader(source_url: str) -> _Response:
                return _Response(b"not-gzip", source_url)

            with self.assertRaises(BatchPipelineError) as raised:
                run_batch_pipeline(
                    "2024-01-01",
                    root,
                    run_id="batch-rejected",
                    downloader=invalid_downloader,
                    now=self.now,
                )

            self.assertEqual(raised.exception.code, "invalid_gzip_source")
            self.assertEqual(accepted.read_bytes(), accepted_bytes)
            self.assertFalse(
                (root / "manifests" / "batch_pipeline" / "partition_date=2024-01-01" / "batch-rejected.json").exists()
            )

    def test_duplicate_and_concurrent_publication_are_no_clobber(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            first = run_batch_pipeline(
                "2024-01-01", root, run_id="batch-duplicate", downloader=self.downloader, now=self.now
            )
            first_bytes = first.read_bytes()
            with self.assertRaises(BatchPipelineError) as repeated:
                run_batch_pipeline(
                    "2024-01-01", root, run_id="batch-duplicate", downloader=self.downloader, now=self.now
                )
            self.assertEqual(repeated.exception.code, "publication_conflict")
            self.assertEqual(first.read_bytes(), first_bytes)

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)

            def publish() -> str:
                try:
                    return str(
                        run_batch_pipeline(
                            "2024-01-01",
                            root,
                            run_id="batch-concurrent",
                            downloader=self.downloader,
                            now=self.now,
                        )
                    )
                except BatchPipelineError as error:
                    return error.code

            with ThreadPoolExecutor(max_workers=2) as executor:
                outcomes = list(executor.map(lambda _: publish(), range(2)))
            self.assertEqual(outcomes.count("publication_conflict"), 1)
            self.assertEqual(sum(outcome.endswith("batch-concurrent.json") for outcome in outcomes), 1)

    def test_interrupted_prepublication_run_recovers_without_rewriting_stage_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with mock.patch("pipelines.batch.local._publish_pipeline_manifest", side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):
                    run_batch_pipeline(
                        "2024-01-01",
                        root,
                        run_id="batch-recovery",
                        downloader=self.downloader,
                        now=self.now,
                    )

            stage_paths = [
                root / "manifests" / "pageviews_hourly" / "partition_date=2024-01-01" / "batch-recovery-bronze.json",
                root / "manifests" / "pageviews_hourly" / "partition_date=2024-01-01" / "batch-recovery-silver.json",
                root / "manifests" / "project_traffic_daily" / "partition_date=2024-01-01" / "batch-recovery-gold.json",
            ]
            stage_digests = [hashlib.sha256(path.read_bytes()).hexdigest() for path in stage_paths]
            final_path = root / "manifests" / "batch_pipeline" / "partition_date=2024-01-01" / "batch-recovery.json"
            self.assertFalse(final_path.exists())

            recovered = run_batch_pipeline(
                "2024-01-01", root, run_id="batch-recovery", downloader=self.downloader, now=self.now
            )

            self.assertEqual(recovered, final_path)
            self.assertEqual(stage_digests, [hashlib.sha256(path.read_bytes()).hexdigest() for path in stage_paths])

    def test_recovery_revalidates_existing_stage_objects_before_publication(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            with mock.patch("pipelines.batch.local._publish_pipeline_manifest", side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):
                    run_batch_pipeline(
                        "2024-01-01",
                        root,
                        run_id="batch-corrupt",
                        downloader=self.downloader,
                        now=self.now,
                    )
            bronze_manifest = (
                root
                / "manifests"
                / "pageviews_hourly"
                / "partition_date=2024-01-01"
                / "batch-corrupt-bronze.json"
            )
            bronze_object = root / self._read(bronze_manifest)["source_objects"][0]["object_path"]
            bronze_object.write_bytes(bronze_object.read_bytes() + b"corrupt")

            with self.assertRaises(BatchPipelineError) as raised:
                run_batch_pipeline(
                    "2024-01-01", root, run_id="batch-corrupt", downloader=self.downloader, now=self.now
                )

            self.assertEqual(raised.exception.code, "broken_stage_join")
            self.assertFalse(
                (root / "manifests" / "batch_pipeline" / "partition_date=2024-01-01" / "batch-corrupt.json").exists()
            )

    def test_fixture_cli_runs_bronze_through_gold_without_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            output = io.StringIO()
            with mock.patch.dict(os.environ, {}, clear=True), contextlib.redirect_stdout(output):
                result = main(
                    [
                        "--partition-date",
                        "2024-01-01",
                        "--source",
                        "fixture",
                        "--destination",
                        str(root),
                        "--run-id",
                        "batch-cli",
                    ]
                )

            self.assertEqual(result, 0)
            manifest_path = Path(output.getvalue().strip())
            self.assertTrue(manifest_path.is_file())
            self.assertEqual(self._read(manifest_path)["run"]["run_id"], "batch-cli")


if __name__ == "__main__":
    unittest.main()
