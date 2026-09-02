"""Run and verify the complete local Wikimedia batch pipeline."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import re
import tempfile
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from data.samples.wikimedia_fixtures import build_fixture_manifest
from pipelines.batch.bronze import (
    BRONZE_MANIFEST_SCHEMA,
    BronzeIngestionError,
    DownloadResponse,
    expected_source_partitions,
    ingest_pageviews,
)
from pipelines.batch.gold import (
    CATALOG_PATH,
    GOLD_MANIFEST_SCHEMA,
    GoldMaterializationError,
    GovernedQueryError,
    materialize_project_traffic_daily,
    open_governed_query,
)
from pipelines.batch.silver import SILVER_MANIFEST_SCHEMA, SilverNormalizationError, normalize_pageviews


BATCH_MANIFEST_SCHEMA = "lakeops/batch-pipeline-manifest@1"
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,54}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class BatchPipelineError(ValueError):
    """A stable failure from the local batch coordinator."""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"[{code}] {detail}")


class _FixtureResponse(io.BytesIO):
    def __init__(self, body: bytes, source_url: str) -> None:
        super().__init__(body)
        self.status = 200
        self.headers = {
            "Content-Length": str(len(body)),
            "ETag": f'"fixture-{hashlib.sha256(source_url.encode("ascii")).hexdigest()[:16]}"',
            "Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT",
        }

    def __enter__(self) -> _FixtureResponse:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


def fixture_downloader(partition_date: str) -> Callable[[str], DownloadResponse]:
    """Return an offline downloader backed by the committed complete-day fixture."""

    try:
        fixture = build_fixture_manifest("complete_day")
    except ValueError as error:
        raise BatchPipelineError("fixture_validation_failure", str(error)) from error
    if fixture.get("partition_date") != partition_date:
        raise BatchPipelineError(
            "fixture_partition_mismatch",
            f"the committed complete-day fixture is for {fixture.get('partition_date')!r}",
        )
    source_objects = fixture.get("source_objects")
    if not isinstance(source_objects, list):
        raise BatchPipelineError("fixture_validation_failure", "complete-day source objects are invalid")
    by_capture_end = {
        item.get("capture_end"): item
        for item in source_objects
        if isinstance(item, Mapping) and isinstance(item.get("capture_end"), str)
    }
    bodies: dict[str, bytes] = {}
    for partition in expected_source_partitions(partition_date, "daily"):
        source = by_capture_end.get(partition.capture_end)
        if not isinstance(source, Mapping) or not isinstance(source.get("records"), list):
            raise BatchPipelineError("fixture_validation_failure", f"fixture lacks {partition.capture_end}")
        lines: list[str] = []
        for record in source["records"]:
            if not isinstance(record, Mapping):
                raise BatchPipelineError("fixture_validation_failure", "fixture record is invalid")
            fields = (
                record.get("domain_code"),
                record.get("page_title"),
                record.get("count_views"),
                record.get("total_response_size"),
            )
            if not all(isinstance(field, str) and field for field in fields):
                raise BatchPipelineError("fixture_validation_failure", "fixture Pageviews fields are invalid")
            lines.append(" ".join(fields))
        bodies[partition.source_url] = gzip.compress(("\n".join(lines) + "\n").encode("utf-8"), mtime=0)

    def download(source_url: str) -> DownloadResponse:
        body = bodies.get(source_url)
        if body is None:
            raise OSError(f"fixture has no pinned source URL {source_url}")
        return _FixtureResponse(body, source_url)

    return download


def run_batch_pipeline(
    partition_date: str,
    destination: Path,
    *,
    run_id: str | None = None,
    downloader: Callable[[str], DownloadResponse] | None = None,
    download_workers: int = 2,
    catalog_path: Path = CATALOG_PATH,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Path:
    """Run Bronze through Gold and publish one authoritative batch manifest.

    The destination is a trusted local POSIX directory. Static symlinks are
    rejected and hard-link publication provides normal-concurrency no-clobber.
    This does not resist a malicious same-user ancestor swap.
    """

    destination = destination.absolute()
    _prepare_destination(destination)
    started_at = _require_clock(now())
    identifier = run_id or _default_run_id(started_at)
    if not RUN_ID_PATTERN.fullmatch(identifier):
        raise BatchPipelineError(
            "invalid_run_id",
            "run_id must be at most 55 characters and contain only letters, digits, '.', '_' or '-'",
        )
    _partition_date(partition_date)
    final_manifest = _pipeline_manifest_path(destination, partition_date, identifier)
    if final_manifest.exists() or final_manifest.is_symlink():
        raise BatchPipelineError("publication_conflict", "batch pipeline manifest already exists")

    bronze_id = f"{identifier}-bronze"
    silver_id = f"{identifier}-silver"
    gold_id = f"{identifier}-gold"
    bronze_manifest = _obtain_stage(
        "bronze",
        _pageviews_manifest_path(destination, partition_date, bronze_id),
        lambda: ingest_pageviews(
            partition_date,
            "daily",
            destination,
            downloader=downloader,
            run_id=bronze_id,
            now=now,
            download_workers=download_workers,
        ),
        BronzeIngestionError,
    )
    silver_manifest = _obtain_stage(
        "silver",
        _pageviews_manifest_path(destination, partition_date, silver_id),
        lambda: normalize_pageviews(
            bronze_manifest,
            destination,
            run_id=silver_id,
            catalog_path=catalog_path,
            now=now,
        ),
        SilverNormalizationError,
    )
    gold_manifest = _obtain_stage(
        "gold",
        _gold_manifest_path(destination, partition_date, gold_id),
        lambda: materialize_project_traffic_daily(
            silver_manifest,
            destination,
            run_id=gold_id,
            catalog_path=catalog_path,
            now=now,
        ),
        GoldMaterializationError,
    )

    stage_manifests, output_objects, counts, durable_paths = _validate_pipeline_evidence(
        destination,
        partition_date,
        bronze_id,
        silver_id,
        gold_id,
        bronze_manifest,
        silver_manifest,
        gold_manifest,
        catalog_path,
    )
    _durability_barrier(durable_paths, destination)
    finished_at = _require_clock(now())
    duration = (finished_at - started_at).total_seconds()
    if duration < 0:
        raise BatchPipelineError("invalid_clock", "now() moved backwards during the batch run")
    manifest = {
        "schema": BATCH_MANIFEST_SCHEMA,
        "status": "accepted",
        "partition_date": partition_date,
        "profile": "daily",
        "stage_manifests": stage_manifests,
        "output_objects": output_objects,
        "run": {
            "run_id": identifier,
            "started_at": _timestamp(started_at),
            "finished_at": _timestamp(finished_at),
            "duration_seconds": duration,
            **counts,
        },
    }
    return _publish_pipeline_manifest(destination, partition_date, identifier, manifest)


def _obtain_stage(
    stage: str,
    expected: Path,
    action: Callable[[], Path],
    error_type: type[BronzeIngestionError] | type[SilverNormalizationError] | type[GoldMaterializationError],
) -> Path:
    if expected.is_symlink():
        raise BatchPipelineError("unsafe_evidence", f"{stage} manifest is a symlink")
    if expected.exists():
        if not expected.is_file():
            raise BatchPipelineError("unsafe_evidence", f"{stage} manifest is not a regular file")
        return expected
    try:
        published = action()
    except error_type as error:
        if error.code == "publication_conflict" and expected.is_file() and not expected.is_symlink():
            return expected
        raise BatchPipelineError(error.code, f"{stage}: {error.detail}") from error
    if published != expected:
        raise BatchPipelineError("invalid_stage_publication", f"{stage} returned a noncanonical manifest path")
    return published


def _validate_pipeline_evidence(
    destination: Path,
    partition_date: str,
    bronze_id: str,
    silver_id: str,
    gold_id: str,
    bronze_path: Path,
    silver_path: Path,
    gold_path: Path,
    catalog_path: Path,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], dict[str, int], list[Path]]:
    bronze_raw, bronze = _load_manifest(bronze_path, destination)
    silver_raw, silver = _load_manifest(silver_path, destination)
    gold_raw, gold = _load_manifest(gold_path, destination)
    _validate_manifest_identity(bronze, BRONZE_MANIFEST_SCHEMA, partition_date, bronze_id)
    _validate_manifest_identity(silver, SILVER_MANIFEST_SCHEMA, partition_date, silver_id)
    _validate_manifest_identity(gold, GOLD_MANIFEST_SCHEMA, partition_date, gold_id)
    if bronze.get("profile") != "daily" or bronze.get("source_id") != "wikimedia_pageviews":
        raise BatchPipelineError("invalid_bronze_manifest", "Bronze manifest is not the daily Wikimedia source")
    if silver.get("dataset_id") != "pageviews_hourly" or gold.get("dataset_id") != "project_traffic_daily":
        raise BatchPipelineError("invalid_stage_manifest", "Silver or Gold dataset identity is invalid")

    bronze_sha = hashlib.sha256(bronze_raw).hexdigest()
    silver_sha = hashlib.sha256(silver_raw).hexdigest()
    gold_sha = hashlib.sha256(gold_raw).hexdigest()
    _validate_join(silver, bronze_path, bronze_id, bronze_sha, destination, "invalid_silver_join")
    _validate_join(gold, silver_path, silver_id, silver_sha, destination, "invalid_gold_join")

    bronze_objects = _object_evidence(
        bronze,
        "source_objects",
        destination,
        "bronze",
        partition_date,
        bronze_id,
        "source_sha256",
        "downloaded_byte_count",
    )
    silver_objects = _object_evidence(
        silver,
        "output_objects",
        destination,
        "silver",
        partition_date,
        silver_id,
        "sha256",
        None,
    )
    gold_objects = _object_evidence(
        gold,
        "output_objects",
        destination,
        "gold",
        partition_date,
        gold_id,
        "sha256",
        None,
    )
    if len(bronze_objects) != 24 or len(silver_objects) != 24 or len(gold_objects) != 1:
        raise BatchPipelineError("incomplete_pipeline", "daily pipeline requires 24 Bronze, 24 Silver, and 1 Gold object")
    if {item["hour"] for item in silver_objects} != set(range(24)):
        raise BatchPipelineError("incomplete_pipeline", "Silver output hours are not the complete UTC day")
    if {item.get("source_object") for item in silver.get("output_objects", []) if isinstance(item, Mapping)} != {
        item.get("object_path") for item in bronze.get("source_objects", []) if isinstance(item, Mapping)
    }:
        raise BatchPipelineError("invalid_silver_join", "Silver objects do not join the complete Bronze object set")
    bronze_run = bronze.get("run")
    silver_run = silver.get("run")
    gold_run = gold.get("run")
    silver_input = silver.get("input_manifest")
    if not isinstance(bronze_run, Mapping) or bronze_run.get("input_object_count") != len(bronze_objects):
        raise BatchPipelineError("invalid_bronze_manifest", "Bronze run count differs from its objects")
    if (
        not isinstance(silver_run, Mapping)
        or silver_run.get("row_count") != sum(item["record_count"] for item in silver_objects)
        or not isinstance(silver_input, Mapping)
        or silver_input.get("source_object_count") != len(bronze_objects)
    ):
        raise BatchPipelineError("invalid_silver_manifest", "Silver run or input count differs from its objects")
    if (
        not isinstance(gold_run, Mapping)
        or gold_run.get("row_count") != sum(item["record_count"] for item in gold_objects)
        or gold.get("input_manifest_ids") != [silver_id]
    ):
        raise BatchPipelineError("invalid_gold_manifest", "Gold run or input identity differs from its objects")

    try:
        with open_governed_query(gold_path, destination, catalog_path=catalog_path) as session:
            rows = session.query("v_project_traffic_daily", ["project_code", "view_count", "input_hour_count", "is_complete"])
    except (GovernedQueryError, GoldMaterializationError) as error:
        raise BatchPipelineError(error.code, f"governed Gold validation failed: {error.detail}") from error
    if not rows or any(row[2] != 24 or row[3] is not True for row in rows):
        raise BatchPipelineError("invalid_gold_output", "governed Gold query is empty or incomplete")

    stage_manifests = {
        "bronze": _manifest_evidence(bronze_path, bronze_sha, bronze_id, destination),
        "silver": _manifest_evidence(silver_path, silver_sha, silver_id, destination),
        "gold": _manifest_evidence(gold_path, gold_sha, gold_id, destination),
    }
    output_objects = [*bronze_objects, *silver_objects, *gold_objects]
    counts = {
        "input_object_count": len(bronze_objects),
        "input_compressed_bytes": sum(item["byte_count"] for item in bronze_objects),
        "source_record_count": sum(item["record_count"] for item in bronze_objects),
        "silver_object_count": len(silver_objects),
        "silver_row_count": sum(item["record_count"] for item in silver_objects),
        "gold_object_count": len(gold_objects),
        "gold_row_count": sum(item["record_count"] for item in gold_objects),
    }
    durable_paths = [bronze_path, silver_path, gold_path, *(destination / item["path"] for item in output_objects)]
    return stage_manifests, output_objects, counts, durable_paths


def _load_manifest(path: Path, destination: Path) -> tuple[bytes, Mapping[str, Any]]:
    _relative_file(path, destination)
    try:
        raw = path.read_bytes()
        document = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BatchPipelineError("invalid_stage_manifest", f"cannot read {path.name}") from error
    if not isinstance(document, Mapping):
        raise BatchPipelineError("invalid_stage_manifest", f"{path.name} is not a JSON object")
    return raw, document


def _validate_manifest_identity(
    document: Mapping[str, Any], schema: str, partition_date: str, run_id: str
) -> None:
    run = document.get("run")
    if (
        document.get("schema") != schema
        or document.get("status") != "accepted"
        or document.get("partition_date") != partition_date
        or not isinstance(run, Mapping)
        or run.get("run_id") != run_id
    ):
        raise BatchPipelineError("invalid_stage_manifest", f"{run_id} has invalid accepted identity")


def _validate_join(
    downstream: Mapping[str, Any],
    upstream_path: Path,
    upstream_id: str,
    upstream_sha: str,
    destination: Path,
    code: str,
) -> None:
    input_manifest = downstream.get("input_manifest")
    expected_path = _relative_file(upstream_path, destination).as_posix()
    if (
        not isinstance(input_manifest, Mapping)
        or input_manifest.get("manifest_id") != upstream_id
        or input_manifest.get("path") != expected_path
        or input_manifest.get("sha256") != upstream_sha
    ):
        raise BatchPipelineError(code, "stage manifest lineage does not match the accepted upstream manifest")


def _object_evidence(
    manifest: Mapping[str, Any],
    field: str,
    destination: Path,
    stage: str,
    partition_date: str,
    run_id: str,
    checksum_field: str,
    declared_size_field: str | None,
) -> list[dict[str, Any]]:
    objects = manifest.get(field)
    if not isinstance(objects, list) or not objects:
        raise BatchPipelineError("invalid_stage_manifest", f"{stage} manifest has no objects")
    evidence: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in objects:
        if not isinstance(item, Mapping):
            raise BatchPipelineError("invalid_stage_manifest", f"{stage} object entry is invalid")
        object_path = item.get("object_path")
        checksum = item.get(checksum_field)
        record_count = item.get("record_count")
        if (
            not isinstance(object_path, str)
            or object_path in seen
            or not isinstance(checksum, str)
            or not SHA256_PATTERN.fullmatch(checksum)
            or not _integer(record_count)
            or record_count <= 0
        ):
            raise BatchPipelineError("invalid_stage_manifest", f"{stage} object metadata is invalid")
        path = destination / object_path
        _relative_file(path, destination)
        hour = _validate_object_identity(stage, item, object_path, partition_date, run_id)
        byte_count = path.stat().st_size
        if declared_size_field is not None and item.get(declared_size_field) != byte_count:
            raise BatchPipelineError("broken_stage_join", f"{stage} object byte count differs from its manifest")
        if _sha256(path) != checksum:
            raise BatchPipelineError("broken_stage_join", f"{stage} object checksum differs from its manifest")
        seen.add(object_path)
        published = {
            "stage": stage,
            "path": object_path,
            "sha256": checksum,
            "byte_count": byte_count,
            "record_count": record_count,
        }
        if hour is not None:
            published["hour"] = hour
        evidence.append(published)
    return evidence


def _validate_object_identity(
    stage: str,
    item: Mapping[str, Any],
    object_path: str,
    partition_date: str,
    run_id: str,
) -> int | None:
    if stage == "bronze":
        logical_hour = _utc_timestamp(item.get("logical_hour"), "invalid_bronze_manifest")
        capture_end = _utc_timestamp(item.get("capture_end"), "invalid_bronze_manifest")
        if logical_hour.date().isoformat() != partition_date or capture_end != logical_hour + timedelta(hours=1):
            raise BatchPipelineError("invalid_bronze_manifest", "Bronze object time identity is invalid")
        expected_name = f"pageviews-{capture_end:%Y%m%d-%H%M%S}.gz"
        expected_path = (
            f"bronze/pageviews/partition_date={partition_date}/hour={logical_hour.hour:02d}/"
            f"run_id={run_id}/{expected_name}"
        )
        expected_urls = {
            partition.capture_end: partition.source_url
            for partition in expected_source_partitions(partition_date, "daily")
        }
        if object_path != expected_path or item.get("source_url") != expected_urls.get(item.get("capture_end")):
            raise BatchPipelineError("invalid_bronze_manifest", "Bronze object path or source URL is noncanonical")
        return logical_hour.hour
    if stage == "silver":
        hour = item.get("hour")
        if not _integer(hour) or not 0 <= hour <= 23 or item.get("partition_date") != partition_date:
            raise BatchPipelineError("invalid_silver_manifest", "Silver object partition identity is invalid")
        capture_end = datetime.combine(date.fromisoformat(partition_date), datetime.min.time(), tzinfo=UTC) + timedelta(
            hours=hour + 1
        )
        expected_path = (
            f"silver/pageviews_hourly/partition_date={partition_date}/hour={hour:02d}/run_id={run_id}/"
            f"pageviews-{capture_end:%Y%m%d-%H%M%S}.parquet"
        )
        if object_path != expected_path:
            raise BatchPipelineError("invalid_silver_manifest", "Silver object path is noncanonical")
        return hour
    expected_path = (
        f"gold/project_traffic_daily/partition_date={partition_date}/run_id={run_id}/project_traffic_daily.parquet"
    )
    if stage != "gold" or object_path != expected_path:
        raise BatchPipelineError("invalid_gold_manifest", "Gold object path is noncanonical")
    return None


def _utc_timestamp(value: object, code: str) -> datetime:
    if not isinstance(value, str):
        raise BatchPipelineError(code, "object timestamp is not a UTC string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise BatchPipelineError(code, "object timestamp is invalid") from error
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise BatchPipelineError(code, "object timestamp is not UTC")
    return parsed


def _manifest_evidence(path: Path, checksum: str, run_id: str, destination: Path) -> dict[str, Any]:
    return {
        "manifest_id": run_id,
        "path": _relative_file(path, destination).as_posix(),
        "sha256": checksum,
        "byte_count": path.stat().st_size,
    }


def _durability_barrier(paths: Sequence[Path], destination: Path) -> None:
    directories: set[Path] = set()
    try:
        for path in paths:
            _relative_file(path, destination)
            descriptor = os.open(path, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            current = path.parent
            while True:
                directories.add(current)
                if current == destination:
                    break
                current = current.parent
        for directory in sorted(directories, key=lambda item: len(item.parts), reverse=True):
            descriptor = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    except OSError as error:
        raise BatchPipelineError("durability_failure", "cannot make accepted stage evidence durable") from error


def _publish_pipeline_manifest(
    destination: Path, partition_date: str, run_id: str, manifest: Mapping[str, Any]
) -> Path:
    parent = destination / "manifests" / "batch_pipeline" / f"partition_date={partition_date}"
    _ensure_directory(parent, destination)
    final = parent / f"{run_id}.json"
    if final.exists() or final.is_symlink():
        raise BatchPipelineError("publication_conflict", "batch pipeline manifest already exists")
    try:
        with tempfile.TemporaryDirectory(prefix="lakeops-batch-manifest-", dir=destination) as temporary_directory:
            staged = Path(temporary_directory) / "manifest.json"
            with staged.open("xb") as output:
                output.write((_canonical_json(manifest) + "\n").encode("ascii"))
                output.flush()
                os.fsync(output.fileno())
            os.link(staged, final)
        descriptor = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except FileExistsError as error:
        raise BatchPipelineError("publication_conflict", "batch pipeline manifest already exists") from error
    except OSError as error:
        if final.exists() and not final.is_symlink():
            final.unlink()
        raise BatchPipelineError("publication_failure", "cannot publish durable batch pipeline manifest") from error
    return final


def _prepare_destination(destination: Path) -> None:
    for candidate in (destination, *destination.parents):
        if candidate.is_symlink():
            raise BatchPipelineError("unsafe_destination", f"destination traverses symlink {candidate}")
    if destination.exists() and not destination.is_dir():
        raise BatchPipelineError("unsafe_destination", "destination is not a directory")
    destination.mkdir(parents=True, exist_ok=True)


def _ensure_directory(path: Path, destination: Path) -> None:
    try:
        relative = path.relative_to(destination)
    except ValueError as error:
        raise BatchPipelineError("unsafe_destination", "publication path escapes destination") from error
    current = destination
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise BatchPipelineError("unsafe_destination", f"destination traverses symlink {current}")
        if current.exists():
            if not current.is_dir():
                raise BatchPipelineError("unsafe_destination", f"publication path is not a directory: {current}")
            continue
        try:
            current.mkdir()
        except FileExistsError:
            if current.is_symlink() or not current.is_dir():
                raise BatchPipelineError("unsafe_destination", f"publication path changed while creating {current}")


def _relative_file(path: Path, destination: Path) -> Path:
    path = path.absolute()
    try:
        relative = path.relative_to(destination)
    except ValueError as error:
        raise BatchPipelineError("unsafe_evidence", "evidence path escapes destination") from error
    current = destination
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise BatchPipelineError("unsafe_evidence", f"evidence traverses symlink {current}")
    if not path.is_file():
        raise BatchPipelineError("missing_evidence", f"referenced evidence is absent: {relative.as_posix()}")
    return relative


def _pageviews_manifest_path(destination: Path, partition_date: str, run_id: str) -> Path:
    return destination / "manifests" / "pageviews_hourly" / f"partition_date={partition_date}" / f"{run_id}.json"


def _gold_manifest_path(destination: Path, partition_date: str, run_id: str) -> Path:
    return destination / "manifests" / "project_traffic_daily" / f"partition_date={partition_date}" / f"{run_id}.json"


def _pipeline_manifest_path(destination: Path, partition_date: str, run_id: str) -> Path:
    return destination / "manifests" / "batch_pipeline" / f"partition_date={partition_date}" / f"{run_id}.json"


def _partition_date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise BatchPipelineError("invalid_partition_date", "partition_date must be YYYY-MM-DD") from error
    if parsed.isoformat() != value:
        raise BatchPipelineError("invalid_partition_date", "partition_date must be YYYY-MM-DD")
    return parsed


def _require_clock(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise BatchPipelineError("invalid_clock", "now() must return a timezone-aware timestamp")
    return value.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    return _require_clock(value).isoformat(timespec="seconds").replace("+00:00", "Z")


def _default_run_id(value: datetime) -> str:
    return f"batch-{_timestamp(value).replace(':', '').replace('-', '')}"


def _integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
    except OSError as error:
        raise BatchPipelineError("evidence_read_failure", f"cannot checksum {path.name}") from error
    return digest.hexdigest()


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the local pipeline using either live Wikimedia or committed fixtures."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--partition-date", required=True, help="UTC logical date in YYYY-MM-DD form")
    parser.add_argument("--source", choices=("fixture", "live"), default="fixture")
    parser.add_argument("--destination", required=True, type=Path, help="trusted local publication root")
    parser.add_argument("--run-id", help="optional immutable pipeline publication identity")
    parser.add_argument("--download-workers", type=int, choices=range(1, 9), default=2)
    arguments = parser.parse_args(argv)
    downloader = fixture_downloader(arguments.partition_date) if arguments.source == "fixture" else None
    manifest = run_batch_pipeline(
        arguments.partition_date,
        arguments.destination,
        run_id=arguments.run_id,
        downloader=downloader,
        download_workers=arguments.download_workers,
    )
    print(manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
