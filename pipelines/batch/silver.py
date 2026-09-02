"""Normalize accepted Wikimedia Pageviews Bronze evidence into Silver Parquet."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import shutil
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import unquote_to_bytes

import duckdb

from data.catalog.validator import CatalogValidationError, validate_catalog


CATALOG_PATH = Path(__file__).resolve().parents[2] / "data" / "catalog" / "catalog.json"
BRONZE_MANIFEST_SCHEMA = "lakeops/bronze-pageviews-manifest@1"
SILVER_MANIFEST_SCHEMA = "lakeops/silver-pageviews-manifest@1"
REJECTION_SCHEMA = "lakeops/silver-pageviews-rejection@1"
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
INVALID_PERCENT_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")
SUPPORTED_PROFILES = frozenset({"tiny", "demo", "daily"})
DUCKDB_MEMORY_LIMIT = "256MB"
PHYSICAL_TYPES = {
    "project_code": "VARCHAR",
    "page_title": "VARCHAR",
    "view_count": "BIGINT",
    "response_bytes": "BIGINT",
    "window_end": "TIMESTAMP",
    "partition_date": "DATE",
    "hour": "INTEGER",
    "source_object": "VARCHAR",
}


class SilverNormalizationError(ValueError):
    """A stable failure from Pageviews Silver normalization."""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"[{code}] {detail}")


@dataclass(frozen=True)
class _PageviewsContract:
    base_url: str
    profile_hours: Mapping[str, int]
    schema: Mapping[str, Mapping[str, Any]]
    daily_expected_hours: int


@dataclass(frozen=True)
class _BronzeObject:
    capture_end: datetime
    logical_hour: datetime
    object_path: str
    source_sha256: str
    source_content_length: int
    record_count: int


@dataclass(frozen=True)
class _StagedOutput:
    source: _BronzeObject
    json_path: Path
    row_count: int


@dataclass(frozen=True)
class _AggregationEvidence:
    peak_temp_directory_bytes: int


def normalize_pageviews(
    bronze_manifest: Path,
    destination: Path,
    *,
    run_id: str | None = None,
    catalog_path: Path = CATALOG_PATH,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Path:
    """Publish one fully validated Silver Pageviews partition as immutable Parquet.

    The local publication boundary is a trusted directory. Static symlinks are
    rejected, and same-filesystem POSIX hard links provide normal-concurrency
    no-clobber behavior. The implementation does not claim resistance to a
    malicious same-user ancestor swap; that requires a native helper.
    """

    destination = destination.absolute()
    bronze_manifest = bronze_manifest.absolute()
    _prepare_destination(destination)
    manifest_relative = _manifest_relative(bronze_manifest, destination)
    _ensure_input_containment(bronze_manifest, destination)
    run_started = _require_clock(now())
    identifier = run_id or _default_run_id(run_started)
    if not RUN_ID_PATTERN.fullmatch(identifier):
        raise SilverNormalizationError("invalid_run_id", "run_id must contain only letters, digits, '.', '_' or '-'")

    raw_manifest = _read_manifest_bytes(bronze_manifest)
    partition_date = "unknown"
    manifest_id: str | None = None
    try:
        document = _parse_manifest(raw_manifest)
        partition_date = _partition_date(document)
        contract = _pageviews_contract(catalog_path)
        manifest_id, source_objects = _validate_bronze_manifest(document, manifest_relative, partition_date, contract)
        with tempfile.TemporaryDirectory(prefix="lakeops-silver-", dir=destination) as temporary_directory:
            staging = Path(temporary_directory)
            staged_outputs = _stage_normalized_records(source_objects, destination, partition_date, contract.schema, staging)
            aggregation_evidence = _assert_no_duplicate_primary_keys(staged_outputs, staging)
            parquet_outputs = _write_and_validate_parquet(staged_outputs, staging)
            return _publish_accepted(
                destination,
                partition_date,
                identifier,
                manifest_relative,
                raw_manifest,
                manifest_id,
                source_objects,
                parquet_outputs,
                aggregation_evidence,
                run_started,
                _require_clock(now()),
            )
    except SilverNormalizationError as error:
        _publish_rejection(
            destination,
            partition_date,
            identifier,
            manifest_relative,
            raw_manifest,
            manifest_id,
            error,
            run_started,
            _require_clock(now()),
        )
        raise


def _manifest_relative(path: Path, destination: Path) -> Path:
    try:
        relative = path.relative_to(destination)
    except ValueError as error:
        raise SilverNormalizationError("unsafe_input", f"manifest escapes destination: {path}") from error
    if relative.is_absolute() or ".." in relative.parts:
        raise SilverNormalizationError("unsafe_input", "manifest path escapes destination")
    return relative


def _read_manifest_bytes(path: Path) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise SilverNormalizationError("invalid_bronze_manifest", "manifest is not a regular file")
    try:
        return path.read_bytes()
    except OSError as error:
        raise SilverNormalizationError("bronze_read_failure", "cannot read manifest") from error


def _parse_manifest(raw_manifest: bytes) -> Mapping[str, Any]:
    try:
        document = json.loads(raw_manifest)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SilverNormalizationError("invalid_bronze_manifest", "manifest is not valid UTF-8 JSON") from error
    if not isinstance(document, Mapping):
        raise SilverNormalizationError("invalid_bronze_manifest", "manifest must be an object")
    return document


def _partition_date(document: Mapping[str, Any]) -> str:
    value = document.get("partition_date")
    if not isinstance(value, str):
        raise SilverNormalizationError("invalid_bronze_manifest", "partition_date must be YYYY-MM-DD")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise SilverNormalizationError("invalid_bronze_manifest", "partition_date must be YYYY-MM-DD") from error
    if parsed.isoformat() != value:
        raise SilverNormalizationError("invalid_bronze_manifest", "partition_date must be YYYY-MM-DD")
    return value


def _pageviews_contract(catalog_path: Path) -> _PageviewsContract:
    try:
        metadata = validate_catalog(json.loads(catalog_path.read_text(encoding="utf-8")))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, CatalogValidationError) as error:
        raise SilverNormalizationError("catalog_contract_failure", str(error)) from error
    source = next((item for item in metadata["sources"] if item["id"] == "wikimedia_pageviews"), None)
    dataset = next((item for item in metadata["datasets"] if item["id"] == "pageviews_hourly"), None)
    if not isinstance(source, Mapping) or not isinstance(dataset, Mapping) or dataset.get("stage") != "silver":
        raise SilverNormalizationError("catalog_contract_failure", "Pageviews source or Silver dataset is absent")
    profiles = {item["id"]: item["pageview_hours"] for item in metadata["volume_profiles"]}
    if set(profiles) < SUPPORTED_PROFILES or any(not isinstance(profiles[name], int) for name in SUPPORTED_PROFILES):
        raise SilverNormalizationError("catalog_contract_failure", "required Pageviews profiles are invalid")
    daily_expected_hours = source["daily_batch"]["expected_hourly_files"]
    if profiles["daily"] != daily_expected_hours or daily_expected_hours != 24:
        raise SilverNormalizationError("catalog_contract_failure", "daily profile must require exactly 24 Pageviews objects")
    definitions = dataset.get("schema")
    if not isinstance(definitions, list):
        raise SilverNormalizationError("catalog_contract_failure", "pageviews_hourly schema is invalid")
    schema = {definition["name"]: definition for definition in definitions if isinstance(definition, Mapping)}
    if set(schema) != set(PHYSICAL_TYPES):
        raise SilverNormalizationError("catalog_contract_failure", "pageviews_hourly fields are not the supported Silver schema")
    return _PageviewsContract(str(source["base_url"]).rstrip("/"), profiles, schema, daily_expected_hours)


def _validate_bronze_manifest(
    document: Mapping[str, Any],
    manifest_relative: Path,
    partition_date: str,
    contract: _PageviewsContract,
) -> tuple[str, list[_BronzeObject]]:
    if document.get("schema") != BRONZE_MANIFEST_SCHEMA:
        raise SilverNormalizationError("invalid_bronze_manifest", f"expected {BRONZE_MANIFEST_SCHEMA!r}")
    if document.get("status") != "accepted" or document.get("source_id") != "wikimedia_pageviews":
        raise SilverNormalizationError("invalid_bronze_manifest", "manifest must be accepted Wikimedia Pageviews evidence")
    manifest_id = document.get("manifest_id")
    profile = document.get("profile")
    run = document.get("run")
    if not isinstance(manifest_id, str) or not RUN_ID_PATTERN.fullmatch(manifest_id):
        raise SilverNormalizationError("invalid_bronze_manifest", "manifest_id is invalid")
    expected_manifest = Path("manifests") / "pageviews_hourly" / f"partition_date={partition_date}" / f"{manifest_id}.json"
    if manifest_relative != expected_manifest:
        raise SilverNormalizationError("invalid_bronze_manifest", "manifest path is not its canonical Bronze manifest identity")
    if profile not in SUPPORTED_PROFILES:
        raise SilverNormalizationError("invalid_bronze_manifest", "manifest profile is unsupported")
    if not isinstance(run, Mapping) or run.get("run_id") != manifest_id:
        raise SilverNormalizationError("invalid_bronze_manifest", "run.run_id must equal manifest_id")
    _required_timestamp(run, "started_at")
    _required_timestamp(run, "finished_at")
    objects = document.get("source_objects")
    expected_count = contract.profile_hours[profile]
    if (
        not isinstance(objects, list)
        or not _is_integer(run.get("input_object_count"))
        or len(objects) != expected_count
        or run["input_object_count"] != expected_count
    ):
        raise SilverNormalizationError("invalid_bronze_manifest", "profile object count does not match the validated contract")

    normalized: list[_BronzeObject] = []
    logical_hours: set[int] = set()
    for item in objects:
        if not isinstance(item, Mapping):
            raise SilverNormalizationError("invalid_bronze_manifest", "source_objects must contain objects")
        capture_end = _parse_timestamp(item.get("capture_end"), "invalid_bronze_manifest")
        logical_hour = _parse_timestamp(item.get("logical_hour"), "invalid_bronze_manifest")
        if capture_end.minute or capture_end.second or capture_end.microsecond:
            raise SilverNormalizationError("invalid_bronze_manifest", "capture_end must be on an hour boundary")
        if capture_end - timedelta(hours=1) != logical_hour:
            raise SilverNormalizationError("invalid_bronze_manifest", "capture_end and logical_hour are inconsistent")
        if logical_hour.date().isoformat() != partition_date:
            raise SilverNormalizationError("invalid_bronze_manifest", "source object crosses the manifest partition")
        source = _validate_source_object(item, partition_date, manifest_id, capture_end, logical_hour, contract)
        if logical_hour.hour in logical_hours:
            raise SilverNormalizationError("invalid_bronze_manifest", "manifest contains duplicate logical hours")
        logical_hours.add(logical_hour.hour)
        normalized.append(source)
    if logical_hours != set(range(expected_count)):
        raise SilverNormalizationError("invalid_bronze_manifest", "manifest logical hours are not the continuous profile hour set")
    return manifest_id, sorted(normalized, key=lambda item: item.logical_hour)


def _validate_source_object(
    item: Mapping[str, Any],
    partition_date: str,
    manifest_id: str,
    capture_end: datetime,
    logical_hour: datetime,
    contract: _PageviewsContract,
) -> _BronzeObject:
    object_path = item.get("object_path")
    source_sha256 = item.get("source_sha256")
    source_content_length = item.get("source_content_length")
    record_count = item.get("record_count")
    if not isinstance(object_path, str) or not object_path:
        raise SilverNormalizationError("invalid_bronze_manifest", "source object has no object_path")
    if not isinstance(source_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", source_sha256):
        raise SilverNormalizationError("invalid_bronze_manifest", "source object has invalid source_sha256")
    if not _is_integer(source_content_length) or source_content_length <= 0:
        raise SilverNormalizationError("invalid_bronze_manifest", "source object has invalid source_content_length")
    if not _is_integer(record_count) or record_count <= 0:
        raise SilverNormalizationError("invalid_bronze_manifest", "source object has invalid record_count")
    expected_url = _source_url(contract.base_url, capture_end)
    if item.get("source_url") != expected_url:
        raise SilverNormalizationError("broken_bronze_join", "source_url is not the canonical Wikimedia object identity")
    if not isinstance(item.get("source_last_modified"), str) or not item["source_last_modified"].strip():
        raise SilverNormalizationError("invalid_bronze_manifest", "source_last_modified must be a non-empty string")
    if not isinstance(item.get("source_etag"), str) or not item["source_etag"].strip():
        raise SilverNormalizationError("invalid_bronze_manifest", "source_etag must be a non-empty string")
    _parse_timestamp(item.get("retrieved_at"), "invalid_bronze_manifest")
    if not _is_integer(item.get("downloaded_byte_count")) or item["downloaded_byte_count"] != source_content_length:
        raise SilverNormalizationError("invalid_bronze_manifest", "downloaded_byte_count must equal source_content_length")
    expected_path = (
        f"bronze/pageviews/partition_date={partition_date}/hour={logical_hour.hour:02d}/"
        f"run_id={manifest_id}/pageviews-{capture_end:%Y%m%d-%H%M%S}.gz"
    )
    if object_path != expected_path:
        raise SilverNormalizationError("broken_bronze_join", "source object path does not join to the accepted manifest identity")
    return _BronzeObject(capture_end, logical_hour, object_path, source_sha256, source_content_length, record_count)


def _stage_normalized_records(
    source_objects: Sequence[_BronzeObject],
    destination: Path,
    partition_date: str,
    schema: Mapping[str, Mapping[str, Any]],
    staging: Path,
) -> list[_StagedOutput]:
    staged_outputs: list[_StagedOutput] = []
    for source in source_objects:
        source_path = _bronze_object_path(destination, source.object_path)
        if source_path.stat().st_size != source.source_content_length or _sha256(source_path) != source.source_sha256:
            raise SilverNormalizationError("broken_bronze_join", "source bytes do not match accepted provenance")
        json_path = staging / f"pageviews-{source.capture_end:%Y%m%d-%H%M%S}.jsonl"
        row_count = _stream_normalize_source(source_path, source, partition_date, schema, json_path)
        if row_count != source.record_count:
            raise SilverNormalizationError("quality_rule_failure", "source record_count differs from accepted manifest")
        staged_outputs.append(_StagedOutput(source, json_path, row_count))
    return staged_outputs


def _bronze_object_path(destination: Path, object_path: str) -> Path:
    relative = Path(object_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise SilverNormalizationError("broken_bronze_join", "source object path escapes destination")
    path = destination / relative
    _ensure_input_containment(path, destination)
    if path.is_symlink() or not path.is_file():
        raise SilverNormalizationError("broken_bronze_join", "source object is not a regular file")
    return path


def _stream_normalize_source(
    source_path: Path,
    source: _BronzeObject,
    partition_date: str,
    schema: Mapping[str, Mapping[str, Any]],
    output_path: Path,
) -> int:
    row_count = 0
    try:
        with gzip.open(source_path, mode="rt", encoding="utf-8", newline="") as raw, output_path.open("x", encoding="utf-8") as output:
            for line_number, line in enumerate(raw, start=1):
                if not line.endswith("\n"):
                    raise SilverNormalizationError("invalid_source_schema", f"{source_path.name}:{line_number} has no newline")
                fields = line[:-1].split(" ")
                if len(fields) != 4 or any(not field for field in fields):
                    raise SilverNormalizationError("invalid_source_schema", f"{source_path.name}:{line_number} must have four fields")
                record = _normalize_record(fields, source, partition_date)
                _validate_record_contract(record, schema)
                output.write(_canonical_json(record) + "\n")
                row_count += 1
    except SilverNormalizationError:
        raise
    except (EOFError, OSError, UnicodeDecodeError) as error:
        raise SilverNormalizationError("invalid_source_schema", f"cannot read {source_path.name}") from error
    if row_count == 0:
        raise SilverNormalizationError("quality_rule_failure", "source object has no records")
    return row_count


def _normalize_record(fields: Sequence[str], source: _BronzeObject, partition_date: str) -> dict[str, Any]:
    domain_code, page_title, count_views, total_response_size = fields
    if any(character.isspace() for character in domain_code):
        raise SilverNormalizationError("invalid_source_schema", "domain_code contains whitespace")
    if not count_views.isdecimal() or int(count_views) <= 0:
        raise SilverNormalizationError("invalid_view_count", "count_views must be a positive integer")
    if not total_response_size.isdecimal():
        raise SilverNormalizationError("invalid_response_bytes", "total_response_size must be a non-negative integer")
    return {
        "project_code": domain_code,
        "page_title": _normalize_page_title(page_title),
        "view_count": int(count_views),
        "response_bytes": int(total_response_size),
        "window_end": _timestamp(source.capture_end),
        "partition_date": partition_date,
        "hour": source.logical_hour.hour,
        "source_object": source.object_path,
    }


def _normalize_page_title(source_title: str) -> str:
    if INVALID_PERCENT_ESCAPE.search(source_title):
        raise SilverNormalizationError("invalid_page_title", "page_title has an invalid percent escape")
    try:
        decoded = unquote_to_bytes(source_title).decode("utf-8")
    except UnicodeDecodeError as error:
        raise SilverNormalizationError("invalid_page_title", "page_title is not valid UTF-8 after decoding") from error
    normalized = decoded.replace(" ", "_")
    if not normalized:
        raise SilverNormalizationError("invalid_page_title", "page_title is empty after normalization")
    return normalized


def _validate_record_contract(record: Mapping[str, Any], schema: Mapping[str, Mapping[str, Any]]) -> None:
    if set(record) != set(schema):
        raise SilverNormalizationError("catalog_contract_failure", "normalized fields do not match pageviews_hourly")
    for name, definition in schema.items():
        value = record[name]
        if definition["type"] in {"string", "timestamp", "date"}:
            valid = isinstance(value, str)
        elif definition["type"] == "integer":
            valid = isinstance(value, int) and not isinstance(value, bool)
        else:
            valid = False
        if not valid or (value is None and not definition["nullable"]):
            raise SilverNormalizationError("catalog_contract_failure", f"{name} violates the catalog type")


def _assert_no_duplicate_primary_keys(outputs: Sequence[_StagedOutput], staging: Path) -> _AggregationEvidence:
    temp_directory = staging / "duckdb-dedupe-tmp"
    profile_path = staging / "duckdb-dedupe-profile.json"
    connection = duckdb.connect()
    try:
        _configure_duckdb(connection, temp_directory)
        connection.execute("SET enable_profiling = 'json'")
        connection.execute(f"SET profiling_output = {_sql_literal(str(profile_path))}")
        input_paths = ",".join(_sql_literal(str(output.json_path)) for output in outputs)
        duplicate = connection.execute(
            "SELECT project_code, page_title, CAST(window_end AS TIMESTAMP) AS window_end "
            f"FROM read_json([{input_paths}]) "
            "GROUP BY project_code, page_title, window_end "
            "HAVING count(*) > 1 "
            "ORDER BY project_code, page_title, window_end LIMIT 1"
        ).fetchone()
        if duplicate is not None:
            project_code, page_title, window_end = duplicate
            raise SilverNormalizationError(
                "duplicate_primary_key",
                f"duplicate pageviews_hourly key {(project_code, page_title, _timestamp(window_end.replace(tzinfo=UTC)))!r}",
            )
        try:
            profile = json.loads(profile_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise SilverNormalizationError("aggregation_profile_failure", "cannot read duplicate aggregation profile") from error
        peak_temp_directory_bytes = profile.get("system_peak_temp_dir_size")
        if not _is_integer(peak_temp_directory_bytes) or peak_temp_directory_bytes < 0:
            raise SilverNormalizationError("aggregation_profile_failure", "duplicate aggregation profile has invalid spill metric")
        return _AggregationEvidence(peak_temp_directory_bytes)
    except duckdb.Error as error:
        raise SilverNormalizationError("parquet_conversion_failure", str(error)) from error
    finally:
        connection.close()


def _write_and_validate_parquet(outputs: Sequence[_StagedOutput], staging: Path) -> list[tuple[_StagedOutput, Path]]:
    connection = duckdb.connect()
    try:
        _configure_duckdb(connection, staging / "duckdb-parquet-tmp")
        parquet_outputs: list[tuple[_StagedOutput, Path]] = []
        for output in outputs:
            parquet_path = staging / f"pageviews-{output.source.capture_end:%Y%m%d-%H%M%S}.parquet"
            source_literal = _sql_literal(str(output.json_path))
            parquet_literal = _sql_literal(str(parquet_path))
            connection.execute(
                "COPY ("
                "SELECT project_code::VARCHAR AS project_code, page_title::VARCHAR AS page_title, "
                "view_count::BIGINT AS view_count, response_bytes::BIGINT AS response_bytes, "
                "CAST(window_end AS TIMESTAMP) AS window_end, CAST(partition_date AS DATE) AS partition_date, "
                "hour::INTEGER AS hour, source_object::VARCHAR AS source_object "
                f"FROM read_json({source_literal})"
                f") TO {parquet_literal} (FORMAT parquet, COMPRESSION zstd)"
            )
            _validate_parquet(connection, parquet_path, output.row_count)
            parquet_outputs.append((output, parquet_path))
        return parquet_outputs
    except duckdb.Error as error:
        raise SilverNormalizationError("parquet_conversion_failure", str(error)) from error
    finally:
        connection.close()


def _validate_parquet(connection: duckdb.DuckDBPyConnection, path: Path, expected_rows: int) -> None:
    literal = _sql_literal(str(path))
    description = connection.execute(f"DESCRIBE SELECT * FROM read_parquet({literal})").fetchall()
    actual_types = {name: field_type.upper() for name, field_type, *_ in description}
    if actual_types != PHYSICAL_TYPES:
        raise SilverNormalizationError("invalid_parquet_schema", f"physical schema differs: {actual_types}")
    row_count = connection.execute(f"SELECT count(*) FROM read_parquet({literal})").fetchone()[0]
    if row_count != expected_rows:
        raise SilverNormalizationError("invalid_parquet_schema", "physical row count differs from staged records")


def _publish_accepted(
    destination: Path,
    partition_date: str,
    run_id: str,
    manifest_relative: Path,
    raw_manifest: bytes,
    manifest_id: str,
    source_objects: Sequence[_BronzeObject],
    outputs: Sequence[tuple[_StagedOutput, Path]],
    aggregation_evidence: _AggregationEvidence,
    started_at: datetime,
    finished_at: datetime,
) -> Path:
    parent = destination / "silver" / "pageviews_hourly" / f"partition_date={partition_date}"
    _ensure_directory(parent, destination)
    claim = parent / ".runs" / f"run_id={run_id}"
    _ensure_directory(claim.parent, destination)
    try:
        claim.mkdir()
    except FileExistsError as error:
        raise SilverNormalizationError("publication_conflict", "Silver run already exists") from error
    published_runs: list[Path] = []
    try:
        output_objects: list[dict[str, Any]] = []
        for staged, parquet_path in outputs:
            source = staged.source
            filename = f"pageviews-{source.capture_end:%Y%m%d-%H%M%S}.parquet"
            object_path = (
                f"silver/pageviews_hourly/partition_date={partition_date}/hour={source.logical_hour.hour:02d}/"
                f"run_id={run_id}/{filename}"
            )
            final_run = parent / f"hour={source.logical_hour.hour:02d}" / f"run_id={run_id}"
            _ensure_directory(final_run.parent, destination)
            final_run.mkdir()
            published_runs.append(final_run)
            os.link(parquet_path, final_run / filename)
            output_objects.append(
                {
                    "partition_date": partition_date,
                    "hour": source.logical_hour.hour,
                    "source_object": source.object_path,
                    "object_path": object_path,
                    "record_count": staged.row_count,
                    "sha256": _sha256(parquet_path),
                    "format": "application/vnd.apache.parquet",
                }
            )
        manifest = {
            "schema": SILVER_MANIFEST_SCHEMA,
            "status": "accepted",
            "dataset_id": "pageviews_hourly",
            "partition_date": partition_date,
            "input_manifest": {
                "manifest_id": manifest_id,
                "path": manifest_relative.as_posix(),
                "sha256": hashlib.sha256(raw_manifest).hexdigest(),
                "source_object_count": len(source_objects),
            },
            "physical_schema": PHYSICAL_TYPES,
            "processing": {
                "duplicate_aggregation": {
                    "duckdb_buffer_memory_limit": DUCKDB_MEMORY_LIMIT,
                    "peak_temp_directory_bytes": aggregation_evidence.peak_temp_directory_bytes,
                }
            },
            "run": {
                "run_id": run_id,
                "started_at": _timestamp(started_at),
                "finished_at": _timestamp(finished_at),
                "row_count": sum(staged.row_count for staged, _ in outputs),
            },
            "output_objects": output_objects,
        }
        manifest_parent = destination / "manifests" / "pageviews_hourly" / f"partition_date={partition_date}"
        _ensure_directory(manifest_parent, destination)
        final_manifest = manifest_parent / f"{run_id}.json"
        if final_manifest.exists() or final_manifest.is_symlink():
            raise SilverNormalizationError("publication_conflict", "Silver manifest already exists")
        with tempfile.TemporaryDirectory(prefix="lakeops-silver-manifest-", dir=destination) as temporary_directory:
            staged_manifest = Path(temporary_directory) / "manifest.json"
            staged_manifest.write_text(_canonical_json(manifest) + "\n", encoding="utf-8")
            os.link(staged_manifest, final_manifest)
        return final_manifest
    except SilverNormalizationError:
        _cleanup_owned(published_runs, claim)
        raise
    except OSError as error:
        _cleanup_owned(published_runs, claim)
        raise SilverNormalizationError("publication_failure", "cannot publish Silver output") from error


def _publish_rejection(
    destination: Path,
    partition_date: str,
    run_id: str,
    manifest_relative: Path,
    raw_manifest: bytes,
    manifest_id: str | None,
    error: SilverNormalizationError,
    started_at: datetime,
    finished_at: datetime,
) -> None:
    parent = destination / "quarantine" / "pageviews_hourly" / f"partition_date={partition_date}"
    _ensure_directory(parent, destination)
    final_run = parent / f"run_id={run_id}"
    try:
        final_run.mkdir()
    except FileExistsError as publish_error:
        raise SilverNormalizationError("publication_conflict", "rejection run already exists") from publish_error
    try:
        evidence = {
            "schema": REJECTION_SCHEMA,
            "status": "rejected",
            "dataset_id": "pageviews_hourly",
            "partition_date": partition_date,
            "error_code": error.code,
            "error_detail": error.detail,
            "input_manifest": {
                "manifest_id": manifest_id,
                "path": manifest_relative.as_posix(),
                "sha256": hashlib.sha256(raw_manifest).hexdigest(),
            },
            "run": {
                "run_id": run_id,
                "started_at": _timestamp(started_at),
                "finished_at": _timestamp(finished_at),
            },
        }
        with tempfile.TemporaryDirectory(prefix="lakeops-silver-rejection-", dir=destination) as temporary_directory:
            staged = Path(temporary_directory) / "rejection.json"
            staged.write_text(_canonical_json(evidence) + "\n", encoding="utf-8")
            os.link(staged, final_run / "rejection.json")
    except OSError as publish_error:
        _cleanup_owned([final_run], None)
        raise SilverNormalizationError("quarantine_publication_failure", "cannot publish rejection evidence") from publish_error


def _source_url(base_url: str, capture_end: datetime) -> str:
    return f"{base_url}/{capture_end:%Y}/{capture_end:%Y-%m}/pageviews-{capture_end:%Y%m%d-%H%M%S}.gz"


def _configure_duckdb(connection: duckdb.DuckDBPyConnection, temp_directory: Path) -> None:
    """Keep DuckDB spill files within the run-owned staging directory."""

    temp_directory.mkdir()
    connection.execute(f"SET memory_limit = {_sql_literal(DUCKDB_MEMORY_LIMIT)}")
    connection.execute(f"SET temp_directory = {_sql_literal(str(temp_directory))}")
    connection.execute("SET threads = 1")
    connection.execute("SET preserve_insertion_order = false")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise SilverNormalizationError("bronze_read_failure", "cannot read source object") from error
    return digest.hexdigest()


def _prepare_destination(destination: Path) -> None:
    for candidate in (destination, *destination.parents):
        if candidate.is_symlink():
            raise SilverNormalizationError("unsafe_destination", "destination traverses a symlink")
    if destination.exists() and not destination.is_dir():
        raise SilverNormalizationError("unsafe_destination", "destination is not a directory")
    destination.mkdir(parents=True, exist_ok=True)


def _ensure_input_containment(path: Path, destination: Path) -> None:
    try:
        relative = path.relative_to(destination)
    except ValueError as error:
        raise SilverNormalizationError("unsafe_input", "input escapes destination") from error
    current = destination
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise SilverNormalizationError("unsafe_input", "input traverses a symlink")


def _ensure_directory(path: Path, destination: Path) -> None:
    try:
        relative = path.relative_to(destination)
    except ValueError as error:
        raise SilverNormalizationError("unsafe_destination", "publication path escapes destination") from error
    current = destination
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise SilverNormalizationError("unsafe_destination", "destination traverses a symlink")
        if current.exists():
            if not current.is_dir():
                raise SilverNormalizationError("unsafe_destination", "publication path is not a directory")
            continue
        try:
            current.mkdir()
        except FileExistsError:
            if current.is_symlink() or not current.is_dir():
                raise SilverNormalizationError("unsafe_destination", "publication path changed while creating it")


def _cleanup_owned(paths: Sequence[Path], claim: Path | None) -> None:
    for path in reversed(paths):
        if path.is_symlink():
            raise SilverNormalizationError("unsafe_destination", "refusing to clean a symlink")
        try:
            shutil.rmtree(path)
        except OSError as error:
            raise SilverNormalizationError("publication_cleanup_failure", "cannot clean incomplete Silver run") from error
    if claim is not None:
        if claim.is_symlink():
            raise SilverNormalizationError("unsafe_destination", "refusing to clean a symlink")
        try:
            claim.rmdir()
        except OSError as error:
            raise SilverNormalizationError("publication_cleanup_failure", "cannot clean Silver run claim") from error


def _required_timestamp(value: Mapping[str, Any], name: str) -> datetime:
    return _parse_timestamp(value.get(name), "invalid_bronze_manifest")


def _is_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _parse_timestamp(value: object, code: str) -> datetime:
    if not isinstance(value, str):
        raise SilverNormalizationError(code, "timestamp must be a UTC string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise SilverNormalizationError(code, "timestamp is invalid") from error
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise SilverNormalizationError(code, "timestamp must be UTC")
    return parsed


def _require_clock(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise SilverNormalizationError("invalid_clock", "clock must be timezone-aware")
    return value


def _timestamp(value: datetime) -> str:
    return _require_clock(value).astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _default_run_id(now_value: datetime) -> str:
    return f"run-{_timestamp(now_value).replace(':', '').replace('-', '')}"


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def main() -> None:
    """Normalize one accepted Bronze Pageviews manifest from the command line."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-manifest", required=True, type=Path)
    parser.add_argument("--destination", required=True, type=Path, help="trusted local publication root")
    parser.add_argument("--run-id", help="optional immutable publication identity")
    arguments = parser.parse_args()
    print(normalize_pageviews(arguments.bronze_manifest, arguments.destination, run_id=arguments.run_id))


if __name__ == "__main__":
    main()
