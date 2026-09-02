"""Materialize governed Pageviews Gold metrics and expose catalog-bound views."""

from __future__ import annotations

import argparse
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

import duckdb

from data.catalog.validator import CatalogValidationError, validate_catalog
from pipelines.batch.silver import PHYSICAL_TYPES, SILVER_MANIFEST_SCHEMA


CATALOG_PATH = Path(__file__).resolve().parents[2] / "data" / "catalog" / "catalog.json"
GOLD_MANIFEST_SCHEMA = "lakeops/gold-project-traffic-manifest@1"
FRESHNESS_MANIFEST_SCHEMA = "lakeops/gold-ingestion-freshness-manifest@1"
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
DUCKDB_MEMORY_LIMIT = "256MB"
DUCKDB_TYPES = {
    "string": "VARCHAR",
    "integer": "BIGINT",
    "timestamp": "TIMESTAMP",
    "date": "DATE",
    "boolean": "BOOLEAN",
    "array_string": "VARCHAR[]",
}


class GoldMaterializationError(ValueError):
    """A stable failure from Gold metric materialization."""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"[{code}] {detail}")


class GovernedQueryError(ValueError):
    """A request outside the catalog-declared DuckDB query surface."""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"[{code}] {detail}")


@dataclass(frozen=True)
class _Contract:
    metadata: Mapping[str, Any]
    datasets: Mapping[str, Mapping[str, Any]]
    views: Mapping[str, Mapping[str, Any]]
    joins: Mapping[str, Mapping[str, Any]]
    daily_expected_hours: int


@dataclass(frozen=True)
class _SilverInput:
    manifest_id: str
    manifest_relative: str
    manifest_sha256: str
    partition_date: str
    paths: tuple[Path, ...]
    rows: int


class GovernedQuerySession:
    """A narrow query surface that accepts only catalog view and field identities."""

    def __init__(self, connection: duckdb.DuckDBPyConnection, contract: _Contract, availability: Mapping[str, bool]) -> None:
        self._connection = connection
        self._contract = contract
        self._availability = availability

    def __enter__(self) -> GovernedQuerySession:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def close(self) -> None:
        self._connection.close()

    def query(self, view_name: str, fields: Sequence[str]) -> list[tuple[Any, ...]]:
        """Return ordered rows for an exact catalog view and its exact catalog fields."""

        view = self._contract.views.get(view_name)
        if view is None:
            raise GovernedQueryError("unknown_view", "view is not registered by the catalog")
        if not self._availability.get(view_name, False):
            raise GovernedQueryError("view_unavailable", "view has no accepted bound catalog input")
        if not isinstance(fields, Sequence) or isinstance(fields, (str, bytes)) or not fields:
            raise GovernedQueryError("invalid_fields", "query requires at least one field")
        allowed = set(view["fields"])
        requested = list(fields)
        if any(not isinstance(field, str) or field not in allowed for field in requested):
            raise GovernedQueryError("unknown_field", "field is not registered for the selected view")
        if len(set(requested)) != len(requested):
            raise GovernedQueryError("invalid_fields", "fields must not repeat")
        quoted = ", ".join(_sql_identifier(field) for field in requested)
        try:
            return self._connection.execute(f"SELECT {quoted} FROM {_sql_identifier(view_name)} ORDER BY {quoted}").fetchall()
        except duckdb.Error as error:
            raise GovernedQueryError("query_failure", "registered view could not be queried") from error


def materialize_project_traffic_daily(
    silver_manifest: Path,
    destination: Path,
    *,
    run_id: str | None = None,
    catalog_path: Path = CATALOG_PATH,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Path:
    """Publish the complete daily traffic KPI from one accepted daily Silver manifest.

    The local publication boundary is a trusted directory. Static symlinks are
    rejected and POSIX hard links give normal-concurrency no-clobber behavior.
    This does not claim resistance to a malicious same-user ancestor swap.
    """

    destination = destination.absolute()
    silver_manifest = silver_manifest.absolute()
    _prepare_destination(destination)
    started_at = _require_clock(now())
    identifier = run_id or _default_run_id(started_at)
    if not RUN_ID_PATTERN.fullmatch(identifier):
        raise GoldMaterializationError("invalid_run_id", "run_id must contain only letters, digits, '.', '_' or '-'")
    contract = _load_contract(catalog_path)
    silver = _load_accepted_silver(silver_manifest, destination, contract)
    if len(silver.paths) != contract.daily_expected_hours:
        raise GoldMaterializationError("incomplete_silver_inputs", "project traffic requires all accepted hourly Silver inputs")

    with tempfile.TemporaryDirectory(prefix="lakeops-gold-", dir=destination) as temporary_directory:
        staging = Path(temporary_directory)
        output_path, row_count = _write_gold_parquet(silver, contract, staging, _require_clock(now()))
        return _publish_gold(
            destination,
            identifier,
            silver,
            contract,
            output_path,
            row_count,
            started_at,
            _require_clock(now()),
        )


def materialize_fixture_ingestion_freshness(
    scenario: str,
    destination: Path,
    *,
    run_id: str | None = None,
    catalog_path: Path = CATALOG_PATH,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Path:
    """Publish bounded source-completeness evidence from a committed fixture scenario."""

    from data.samples.wikimedia_fixtures import build_fixture_manifest, canonical_json

    destination = destination.absolute()
    _prepare_destination(destination)
    started_at = _require_clock(now())
    identifier = run_id or _default_run_id(started_at)
    if not RUN_ID_PATTERN.fullmatch(identifier):
        raise GoldMaterializationError("invalid_run_id", "run_id must contain only letters, digits, '.', '_' or '-'")
    contract = _load_contract(catalog_path)
    fixture = build_fixture_manifest(scenario)
    partition_date = fixture["partition_date"]
    accepted_count = len(fixture["source_objects"])
    expected_count = contract.daily_expected_hours
    if accepted_count > expected_count:
        raise GoldMaterializationError("invalid_fixture_evidence", "fixture has more source hours than the daily contract")
    status = "complete" if accepted_count == expected_count else "missing"
    with tempfile.TemporaryDirectory(prefix="lakeops-freshness-", dir=destination) as temporary_directory:
        staging = Path(temporary_directory)
        output = staging / "ingestion_freshness.parquet"
        connection = duckdb.connect()
        try:
            _configure_duckdb(connection, staging / "duckdb-freshness-tmp")
            fixture_digest = hashlib.sha256(canonical_json(fixture).encode("ascii")).hexdigest()
            connection.execute(
                "COPY (SELECT "
                "'wikimedia_pageviews'::VARCHAR AS source_id, "
                f"{_sql_literal(partition_date)}::VARCHAR AS logical_partition, "
                f"{expected_count}::BIGINT AS expected_count, {accepted_count}::BIGINT AS accepted_count, "
                f"{_sql_literal(status)}::VARCHAR AS freshness_status, 0::BIGINT AS lag_seconds, "
                "CAST(NULL AS VARCHAR) AS last_successful_run_id, "
                f"{_sql_literal(identifier)}::VARCHAR AS current_manifest_id, {_sql_literal(partition_date)}::DATE AS partition_date"
                f") TO {_sql_literal(str(output))} (FORMAT parquet, COMPRESSION zstd)"
            )
            if {name: field_type.upper() for name, field_type, *_ in connection.execute(f"DESCRIBE SELECT * FROM read_parquet({_sql_literal(str(output))})").fetchall()} != _physical_types(contract.datasets["ingestion_freshness"]):
                raise GoldMaterializationError("invalid_gold_schema", "freshness output schema differs from catalog")
        except duckdb.Error as error:
            raise GoldMaterializationError("gold_materialization_failure", "DuckDB could not produce freshness evidence") from error
        finally:
            connection.close()
        return _publish_freshness(destination, identifier, partition_date, scenario, fixture_digest, accepted_count, expected_count, output, started_at, _require_clock(now()))


def open_governed_query(gold_manifest: Path, destination: Path, *, catalog_path: Path = CATALOG_PATH) -> GovernedQuerySession:
    """Open a session whose public interface is limited to catalog view/field allowlists."""

    destination = destination.absolute()
    gold_manifest = gold_manifest.absolute()
    _prepare_destination(destination)
    contract = _load_contract(catalog_path)
    try:
        _, document, _ = _read_canonical_manifest(gold_manifest, destination, "unsafe_input")
        if document.get("schema") == FRESHNESS_MANIFEST_SCHEMA:
            gold, silver, freshness = None, None, _load_accepted_freshness(gold_manifest, destination, contract)
        else:
            gold, silver, freshness = (*_load_accepted_gold(gold_manifest, destination, contract), None)
    except GoldMaterializationError as error:
        raise GovernedQueryError(error.code, error.detail) from error
    connection = duckdb.connect()
    temporary: Path | None = None
    try:
        temporary_parent = destination / ".duckdb-query"
        _ensure_directory(temporary_parent, destination)
        temporary = Path(tempfile.mkdtemp(prefix="session-", dir=temporary_parent))
        _configure_duckdb(connection, temporary)
        availability = _register_catalog_views(connection, contract, gold, silver, freshness)
        return _ManagedGovernedQuerySession(connection, contract, availability, temporary)
    except Exception:
        connection.close()
        if temporary is not None and temporary.exists() and not temporary.is_symlink():
            shutil.rmtree(temporary)
        raise


class _ManagedGovernedQuerySession(GovernedQuerySession):
    def __init__(self, connection: duckdb.DuckDBPyConnection, contract: _Contract, availability: Mapping[str, bool], temporary: Path) -> None:
        super().__init__(connection, contract, availability)
        self._temporary = temporary
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._connection is not None:
            self._connection.close()
            self._connection = None  # type: ignore[assignment]
        if self._temporary.exists() and not self._temporary.is_symlink():
            shutil.rmtree(self._temporary)


def _load_contract(catalog_path: Path) -> _Contract:
    try:
        metadata = validate_catalog(json.loads(catalog_path.read_text(encoding="utf-8")))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, CatalogValidationError) as error:
        raise GoldMaterializationError("catalog_contract_failure", str(error)) from error
    datasets = {item["id"]: item for item in metadata["datasets"]}
    views = metadata["query_surface"]["view_contracts"]
    joins = {item["id"]: item for item in metadata["joins"]}
    pageviews = datasets.get("pageviews_hourly")
    gold = datasets.get("project_traffic_daily")
    if pageviews is None or gold is None or gold.get("stage") != "gold":
        raise GoldMaterializationError("catalog_contract_failure", "required Pageviews datasets are absent")
    source = next((item for item in metadata["sources"] if item["id"] == "wikimedia_pageviews"), None)
    if not isinstance(source, Mapping) or source["daily_batch"]["expected_hourly_files"] != 24:
        raise GoldMaterializationError("catalog_contract_failure", "daily Pageviews completeness is invalid")
    required_views = {"v_project_traffic_daily", "v_page_activity_hourly", "v_page_traffic_activity", "v_ingestion_freshness", "v_pipeline_runs"}
    if set(views) != required_views:
        raise GoldMaterializationError("catalog_contract_failure", "query surface differs from the supported governed views")
    return _Contract(metadata, datasets, views, joins, source["daily_batch"]["expected_hourly_files"])


def _load_accepted_silver(path: Path, destination: Path, contract: _Contract) -> _SilverInput:
    raw, document, relative = _read_canonical_manifest(path, destination, "unsafe_input")
    partition_date = _partition_date(document, "invalid_silver_manifest")
    manifest_id = document.get("run", {}).get("run_id") if isinstance(document.get("run"), Mapping) else None
    expected = Path("manifests") / "pageviews_hourly" / f"partition_date={partition_date}" / f"{manifest_id}.json"
    if relative != expected or document.get("schema") != SILVER_MANIFEST_SCHEMA or document.get("status") != "accepted":
        raise GoldMaterializationError("invalid_silver_manifest", "manifest is not canonical accepted Pageviews Silver evidence")
    if document.get("dataset_id") != "pageviews_hourly" or not isinstance(manifest_id, str) or not RUN_ID_PATTERN.fullmatch(manifest_id):
        raise GoldMaterializationError("invalid_silver_manifest", "Silver manifest identity is invalid")
    if document.get("physical_schema") != PHYSICAL_TYPES:
        raise GoldMaterializationError("invalid_silver_manifest", "Silver physical schema differs from the contract")
    objects = document.get("output_objects")
    if not isinstance(objects, list) or not objects:
        raise GoldMaterializationError("incomplete_silver_inputs", "Silver manifest has no output objects")
    expected_hours = set(range(len(objects)))
    actual_hours: set[int] = set()
    paths: list[Path] = []
    rows = 0
    for item in objects:
        if not isinstance(item, Mapping):
            raise GoldMaterializationError("invalid_silver_manifest", "Silver output object is invalid")
        hour = item.get("hour")
        object_path = item.get("object_path")
        sha256 = item.get("sha256")
        record_count = item.get("record_count")
        if not _integer(hour) or hour < 0 or hour > 23 or not isinstance(object_path, str) or not re.fullmatch(r"[0-9a-f]{64}", str(sha256)) or not _integer(record_count) or record_count <= 0:
            raise GoldMaterializationError("invalid_silver_manifest", "Silver output metadata is invalid")
        expected_path = Path("silver") / "pageviews_hourly" / f"partition_date={partition_date}" / f"hour={hour:02d}" / f"run_id={manifest_id}" / f"pageviews-{_capture_label(partition_date, hour)}.parquet"
        if Path(object_path) != expected_path or hour in actual_hours:
            raise GoldMaterializationError("broken_silver_join", "Silver output path is not a canonical unique object identity")
        output = _contained_regular_file(destination, Path(object_path), "broken_silver_join")
        if _sha256(output) != sha256:
            raise GoldMaterializationError("broken_silver_join", "Silver output checksum differs from accepted manifest")
        actual_hours.add(hour)
        paths.append(output)
        rows += record_count
    if actual_hours != expected_hours:
        raise GoldMaterializationError("incomplete_silver_inputs", "Silver output hours are not continuous")
    _validate_silver_parquet(paths, objects)
    run = document.get("run")
    if not isinstance(run, Mapping) or run.get("row_count") != rows:
        raise GoldMaterializationError("invalid_silver_manifest", "Silver run row_count differs from output objects")
    return _SilverInput(manifest_id, relative.as_posix(), hashlib.sha256(raw).hexdigest(), partition_date, tuple(paths), rows)


def _write_gold_parquet(silver: _SilverInput, contract: _Contract, staging: Path, computed_at: datetime) -> tuple[Path, int]:
    output = staging / "project_traffic_daily.parquet"
    connection = duckdb.connect()
    try:
        _configure_duckdb(connection, staging / "duckdb-gold-tmp")
        paths = ", ".join(_sql_literal(str(path)) for path in silver.paths)
        manifest_ids = _sql_literal(silver.manifest_id)
        computed = _sql_literal(_timestamp(computed_at))
        connection.execute(
            "COPY ("
            "SELECT project_code::VARCHAR AS project_code, partition_date::DATE AS partition_date, "
            "sum(view_count)::BIGINT AS view_count, sum(response_bytes)::BIGINT AS response_bytes, "
            "count(DISTINCT page_title)::BIGINT AS unique_page_count, count(DISTINCT hour)::INTEGER AS input_hour_count, "
            f"count(DISTINCT hour) = {contract.daily_expected_hours} AS is_complete, "
            f"[{manifest_ids}]::VARCHAR[] AS input_manifest_ids, CAST({computed} AS TIMESTAMP) AS computed_at "
            f"FROM read_parquet([{paths}], hive_partitioning = false) "
            "GROUP BY project_code, partition_date"
            f") TO {_sql_literal(str(output))} (FORMAT parquet, COMPRESSION zstd)"
        )
        expected = _physical_types(contract.datasets["project_traffic_daily"])
        actual = {name: type_name.upper() for name, type_name, *_ in connection.execute(f"DESCRIBE SELECT * FROM read_parquet({_sql_literal(str(output))})").fetchall()}
        if actual != expected:
            raise GoldMaterializationError("invalid_gold_schema", f"Gold output schema differs from catalog: {actual!r}")
        rows = connection.execute(f"SELECT count(*) FROM read_parquet({_sql_literal(str(output))})").fetchone()[0]
        if not _integer(rows) or rows <= 0:
            raise GoldMaterializationError("empty_gold_output", "Gold metrics cannot be empty for accepted Pageviews input")
        return output, rows
    except duckdb.Error as error:
        raise GoldMaterializationError("gold_materialization_failure", "DuckDB could not produce governed Gold output") from error
    finally:
        connection.close()


def _publish_gold(destination: Path, run_id: str, silver: _SilverInput, contract: _Contract, staged: Path, row_count: int, started_at: datetime, finished_at: datetime) -> Path:
    parent = destination / "gold" / "project_traffic_daily" / f"partition_date={silver.partition_date}"
    _ensure_directory(parent, destination)
    run_directory = parent / f"run_id={run_id}"
    if run_directory.exists() or run_directory.is_symlink():
        raise GoldMaterializationError("publication_conflict", "Gold run already exists")
    manifest_parent = destination / "manifests" / "project_traffic_daily" / f"partition_date={silver.partition_date}"
    _ensure_directory(manifest_parent, destination)
    manifest_path = manifest_parent / f"{run_id}.json"
    if manifest_path.exists() or manifest_path.is_symlink():
        raise GoldMaterializationError("publication_conflict", "Gold manifest already exists")
    output_name = "project_traffic_daily.parquet"
    output_relative = Path("gold") / "project_traffic_daily" / f"partition_date={silver.partition_date}" / f"run_id={run_id}" / output_name
    manifest = {
        "schema": GOLD_MANIFEST_SCHEMA,
        "status": "accepted",
        "dataset_id": "project_traffic_daily",
        "partition_date": silver.partition_date,
        "kpi": {"id": "kpi.project_daily_views", "unit": "views", "formula": _kpi_formula(contract, "kpi.project_daily_views")},
        "freshness": {"definition": contract.datasets["project_traffic_daily"]["freshness"], "is_complete": True},
        "input_manifest": {"dataset_id": "pageviews_hourly", "manifest_id": silver.manifest_id, "path": silver.manifest_relative, "sha256": silver.manifest_sha256},
        "input_manifest_ids": [silver.manifest_id],
        "processing": {"input_hour_count": contract.daily_expected_hours, "duckdb_buffer_memory_limit": DUCKDB_MEMORY_LIMIT},
        "run": {"run_id": run_id, "started_at": _timestamp(started_at), "finished_at": _timestamp(finished_at), "row_count": row_count},
        "output_objects": [{"object_path": output_relative.as_posix(), "record_count": row_count, "sha256": _sha256(staged), "format": "application/vnd.apache.parquet"}],
    }
    claim = parent / ".claims" / f"run_id={run_id}"
    _ensure_directory(claim.parent, destination)
    try:
        claim.mkdir()
    except FileExistsError as error:
        raise GoldMaterializationError("publication_conflict", "Gold run is already claimed") from error
    run_owned = False
    try:
        run_directory.mkdir()
        run_owned = True
        os.link(staged, run_directory / output_name)
        with tempfile.TemporaryDirectory(prefix="lakeops-gold-manifest-", dir=destination) as temporary_directory:
            candidate = Path(temporary_directory) / "manifest.json"
            candidate.write_text(_canonical_json(manifest) + "\n", encoding="utf-8")
            os.link(candidate, manifest_path)
        return manifest_path
    except FileExistsError as error:
        if run_owned and run_directory.exists() and not run_directory.is_symlink():
            shutil.rmtree(run_directory)
        if claim.exists() and not claim.is_symlink():
            claim.rmdir()
        raise GoldMaterializationError("publication_conflict", "Gold publication target already exists") from error
    except OSError as error:
        if run_owned and run_directory.exists() and not run_directory.is_symlink():
            shutil.rmtree(run_directory)
        if claim.exists() and not claim.is_symlink():
            claim.rmdir()
        raise GoldMaterializationError("publication_failure", "cannot publish immutable Gold output") from error


def _load_accepted_gold(path: Path, destination: Path, contract: _Contract) -> tuple[Path, _SilverInput]:
    _, document, relative = _read_canonical_manifest(path, destination, "unsafe_input")
    partition_date = _partition_date(document, "invalid_gold_manifest")
    run = document.get("run")
    run_id = run.get("run_id") if isinstance(run, Mapping) else None
    expected = Path("manifests") / "project_traffic_daily" / f"partition_date={partition_date}" / f"{run_id}.json"
    if relative != expected or document.get("schema") != GOLD_MANIFEST_SCHEMA or document.get("status") != "accepted" or document.get("dataset_id") != "project_traffic_daily" or not isinstance(run_id, str) or not RUN_ID_PATTERN.fullmatch(run_id):
        raise GovernedQueryError("invalid_gold_manifest", "Gold manifest is not canonical accepted evidence")
    input_manifest = document.get("input_manifest")
    if not isinstance(input_manifest, Mapping) or input_manifest.get("dataset_id") != "pageviews_hourly" or not isinstance(input_manifest.get("path"), str) or not re.fullmatch(r"[0-9a-f]{64}", str(input_manifest.get("sha256"))):
        raise GovernedQueryError("invalid_gold_manifest", "Gold manifest lacks a governed Silver input")
    silver = _load_accepted_silver(destination / input_manifest["path"], destination, contract)
    if silver.partition_date != partition_date or input_manifest.get("manifest_id") != silver.manifest_id or input_manifest.get("sha256") != silver.manifest_sha256:
        raise GovernedQueryError("broken_gold_join", "Gold manifest does not join its accepted Silver input")
    objects = document.get("output_objects")
    if not isinstance(objects, list) or len(objects) != 1 or not isinstance(objects[0], Mapping):
        raise GovernedQueryError("invalid_gold_manifest", "Gold manifest must have one output object")
    output = objects[0]
    expected_output = Path("gold") / "project_traffic_daily" / f"partition_date={partition_date}" / f"run_id={run_id}" / "project_traffic_daily.parquet"
    if output.get("object_path") != expected_output.as_posix() or not re.fullmatch(r"[0-9a-f]{64}", str(output.get("sha256"))):
        raise GovernedQueryError("broken_gold_join", "Gold output has no canonical identity")
    output_path = _contained_regular_file(destination, expected_output, "broken_gold_join")
    if _sha256(output_path) != output["sha256"]:
        raise GovernedQueryError("broken_gold_join", "Gold output checksum differs from manifest")
    kpi = document.get("kpi")
    if not isinstance(kpi, Mapping) or kpi.get("id") != "kpi.project_daily_views" or kpi.get("unit") != "views" or kpi.get("formula") != _kpi_formula(contract, "kpi.project_daily_views"):
        raise GovernedQueryError("invalid_gold_manifest", "Gold KPI definition differs from the catalog")
    processing = document.get("processing")
    if not isinstance(processing, Mapping) or processing.get("input_hour_count") != contract.daily_expected_hours:
        raise GovernedQueryError("invalid_gold_manifest", "Gold completeness definition differs from the catalog")
    freshness = document.get("freshness")
    if not isinstance(freshness, Mapping) or freshness.get("definition") != contract.datasets["project_traffic_daily"]["freshness"] or freshness.get("is_complete") is not True:
        raise GovernedQueryError("invalid_gold_manifest", "Gold freshness definition differs from the catalog")
    _validate_gold_parquet(output_path, contract, output.get("record_count"))
    if not isinstance(run, Mapping) or run.get("row_count") != output.get("record_count"):
        raise GovernedQueryError("invalid_gold_manifest", "Gold run row_count differs from output object")
    return output_path, silver


def _load_accepted_freshness(path: Path, destination: Path, contract: _Contract) -> Path:
    from data.samples.wikimedia_fixtures import build_fixture_manifest, canonical_json

    _, document, relative = _read_canonical_manifest(path, destination, "unsafe_input")
    partition_date = _partition_date(document, "invalid_freshness_manifest")
    run = document.get("run")
    run_id = run.get("run_id") if isinstance(run, Mapping) else None
    expected = Path("manifests") / "ingestion_freshness" / f"partition_date={partition_date}" / f"{run_id}.json"
    if relative != expected or document.get("schema") != FRESHNESS_MANIFEST_SCHEMA or document.get("status") != "accepted" or document.get("dataset_id") != "ingestion_freshness" or not isinstance(run_id, str) or not RUN_ID_PATTERN.fullmatch(run_id):
        raise GovernedQueryError("invalid_freshness_manifest", "freshness manifest is not canonical accepted evidence")
    evidence = document.get("fixture_evidence")
    if not isinstance(evidence, Mapping) or not isinstance(evidence.get("scenario"), str) or not re.fullmatch(r"[0-9a-f]{64}", str(evidence.get("sha256"))) or not _integer(evidence.get("expected_count")) or not _integer(evidence.get("accepted_count")):
        raise GovernedQueryError("invalid_freshness_manifest", "freshness manifest lacks committed fixture lineage")
    fixture = build_fixture_manifest(evidence["scenario"])
    if (
        evidence["sha256"] != hashlib.sha256(canonical_json(fixture).encode("ascii")).hexdigest()
        or evidence["expected_count"] != contract.daily_expected_hours
        or evidence["accepted_count"] != len(fixture["source_objects"])
    ):
        raise GovernedQueryError("broken_freshness_join", "freshness evidence no longer matches its committed fixture")
    objects = document.get("output_objects")
    if not isinstance(objects, list) or len(objects) != 1 or not isinstance(objects[0], Mapping):
        raise GovernedQueryError("invalid_freshness_manifest", "freshness manifest must have one output object")
    output = objects[0]
    expected_output = Path("gold") / "ingestion_freshness" / f"partition_date={partition_date}" / f"run_id={run_id}" / "ingestion_freshness.parquet"
    if output.get("object_path") != expected_output.as_posix() or not re.fullmatch(r"[0-9a-f]{64}", str(output.get("sha256"))):
        raise GovernedQueryError("broken_freshness_join", "freshness output has no canonical identity")
    output_path = _contained_regular_file(destination, expected_output, "broken_freshness_join")
    if _sha256(output_path) != output["sha256"]:
        raise GovernedQueryError("broken_freshness_join", "freshness output checksum differs from manifest")
    _validate_dataset_parquet(output_path, contract.datasets["ingestion_freshness"], output.get("record_count"), "broken_freshness_join")
    if not isinstance(run, Mapping) or run.get("row_count") != output.get("record_count"):
        raise GovernedQueryError("invalid_freshness_manifest", "freshness run row_count differs from output object")
    return output_path


def _register_catalog_views(
    connection: duckdb.DuckDBPyConnection,
    contract: _Contract,
    gold: Path | None,
    silver: _SilverInput | None,
    freshness: Path | None,
) -> dict[str, bool]:
    availability = {name: False for name in contract.views}
    if gold is not None and silver is not None:
        traffic = _read_parquet_sql([gold])
        fields = contract.views["v_project_traffic_daily"]["fields"]
        connection.execute(f"CREATE VIEW v_project_traffic_daily AS SELECT {_field_projection(fields, traffic)} FROM {traffic}")
        availability["v_project_traffic_daily"] = True
    if freshness is not None:
        source = _read_parquet_sql([freshness])
        fields = contract.views["v_ingestion_freshness"]["fields"]
        connection.execute(f"CREATE VIEW v_ingestion_freshness AS SELECT {_field_projection(fields, source)} FROM {source}")
        availability["v_ingestion_freshness"] = True
    return {
        name: availability.get(name, False) for name in contract.views
    }


def _publish_freshness(
    destination: Path,
    run_id: str,
    partition_date: str,
    scenario: str,
    fixture_sha256: str,
    accepted_count: int,
    expected_count: int,
    staged: Path,
    started_at: datetime,
    finished_at: datetime,
) -> Path:
    parent = destination / "gold" / "ingestion_freshness" / f"partition_date={partition_date}"
    manifest_parent = destination / "manifests" / "ingestion_freshness" / f"partition_date={partition_date}"
    _ensure_directory(parent, destination)
    _ensure_directory(manifest_parent, destination)
    claim = parent / ".claims" / f"run_id={run_id}"
    _ensure_directory(claim.parent, destination)
    try:
        claim.mkdir()
    except FileExistsError as error:
        raise GoldMaterializationError("publication_conflict", "freshness run is already claimed") from error
    run_directory = parent / f"run_id={run_id}"
    manifest_path = manifest_parent / f"{run_id}.json"
    run_owned = False
    try:
        if manifest_path.exists() or manifest_path.is_symlink():
            raise FileExistsError(manifest_path)
        run_directory.mkdir()
        run_owned = True
        output_name = "ingestion_freshness.parquet"
        os.link(staged, run_directory / output_name)
        output_relative = Path("gold") / "ingestion_freshness" / f"partition_date={partition_date}" / f"run_id={run_id}" / output_name
        manifest = {
            "schema": FRESHNESS_MANIFEST_SCHEMA,
            "status": "accepted",
            "dataset_id": "ingestion_freshness",
            "partition_date": partition_date,
            "fixture_evidence": {"scenario": scenario, "sha256": fixture_sha256, "expected_count": expected_count, "accepted_count": accepted_count},
            "run": {"run_id": run_id, "started_at": _timestamp(started_at), "finished_at": _timestamp(finished_at), "row_count": 1},
            "output_objects": [{"object_path": output_relative.as_posix(), "record_count": 1, "sha256": _sha256(staged), "format": "application/vnd.apache.parquet"}],
        }
        with tempfile.TemporaryDirectory(prefix="lakeops-freshness-manifest-", dir=destination) as temporary_directory:
            candidate = Path(temporary_directory) / "manifest.json"
            candidate.write_text(_canonical_json(manifest) + "\n", encoding="utf-8")
            os.link(candidate, manifest_path)
        return manifest_path
    except FileExistsError as error:
        if run_owned and run_directory.exists() and not run_directory.is_symlink():
            shutil.rmtree(run_directory)
        if claim.exists() and not claim.is_symlink():
            claim.rmdir()
        raise GoldMaterializationError("publication_conflict", "freshness publication target already exists") from error
    except OSError as error:
        if run_owned and run_directory.exists() and not run_directory.is_symlink():
            shutil.rmtree(run_directory)
        if claim.exists() and not claim.is_symlink():
            claim.rmdir()
        raise GoldMaterializationError("publication_failure", "cannot publish immutable freshness evidence") from error


def _validate_silver_parquet(paths: Sequence[Path], objects: Sequence[Mapping[str, Any]]) -> None:
    connection = duckdb.connect()
    try:
        for path, object_metadata in zip(paths, objects, strict=True):
            literal = _sql_literal(str(path))
            actual = {name: field_type.upper() for name, field_type, *_ in connection.execute(f"DESCRIBE SELECT * FROM read_parquet({literal}, hive_partitioning = false)").fetchall()}
            if actual != PHYSICAL_TYPES:
                raise GoldMaterializationError("broken_silver_join", "Silver Parquet schema differs from accepted manifest")
            row_count = connection.execute(f"SELECT count(*) FROM read_parquet({literal}, hive_partitioning = false)").fetchone()[0]
            if row_count != object_metadata["record_count"]:
                raise GoldMaterializationError("broken_silver_join", "Silver Parquet row count differs from accepted manifest")
    except duckdb.Error as error:
        raise GoldMaterializationError("broken_silver_join", "Silver Parquet cannot be read") from error
    finally:
        connection.close()


def _validate_gold_parquet(path: Path, contract: _Contract, expected_rows: object) -> None:
    _validate_dataset_parquet(path, contract.datasets["project_traffic_daily"], expected_rows, "broken_gold_join")


def _validate_dataset_parquet(path: Path, dataset: Mapping[str, Any], expected_rows: object, code: str) -> None:
    if not _integer(expected_rows) or expected_rows <= 0:
        raise GovernedQueryError("invalid_gold_manifest", "Gold output row count is invalid")
    connection = duckdb.connect()
    try:
        literal = _sql_literal(str(path))
        actual = {name: field_type.upper() for name, field_type, *_ in connection.execute(f"DESCRIBE SELECT * FROM read_parquet({literal}, hive_partitioning = false)").fetchall()}
        if actual != _physical_types(dataset):
            raise GovernedQueryError(code, "Gold Parquet schema differs from the catalog")
        row_count = connection.execute(f"SELECT count(*) FROM read_parquet({literal}, hive_partitioning = false)").fetchone()[0]
        if row_count != expected_rows:
            raise GovernedQueryError(code, "Gold Parquet row count differs from accepted manifest")
    except duckdb.Error as error:
        raise GovernedQueryError(code, "Gold Parquet cannot be read") from error
    finally:
        connection.close()


def _read_canonical_manifest(path: Path, destination: Path, error_code: str) -> tuple[bytes, Mapping[str, Any], Path]:
    try:
        relative = path.relative_to(destination)
    except ValueError as error:
        raise GoldMaterializationError(error_code, "manifest escapes trusted destination") from error
    if relative.is_absolute() or ".." in relative.parts:
        raise GoldMaterializationError(error_code, "manifest escapes trusted destination")
    checked = _contained_regular_file(destination, relative, error_code)
    try:
        raw = checked.read_bytes()
        document = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise GoldMaterializationError("invalid_silver_manifest", "manifest is not valid UTF-8 JSON") from error
    if not isinstance(document, Mapping):
        raise GoldMaterializationError("invalid_silver_manifest", "manifest must be a JSON object")
    return raw, document, relative


def _contained_regular_file(destination: Path, relative: Path, code: str) -> Path:
    if relative.is_absolute() or ".." in relative.parts:
        raise GoldMaterializationError(code, "path escapes trusted destination")
    candidate = destination / relative
    current = destination
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise GoldMaterializationError(code, "path traverses a symlink")
    if candidate.is_symlink() or not candidate.is_file():
        raise GoldMaterializationError(code, "path is not a regular file")
    return candidate


def _prepare_destination(destination: Path) -> None:
    for candidate in (destination, *destination.parents):
        if candidate.is_symlink():
            raise GoldMaterializationError("unsafe_destination", "destination traverses a symlink")
    if destination.exists() and not destination.is_dir():
        raise GoldMaterializationError("unsafe_destination", "destination is not a directory")
    destination.mkdir(parents=True, exist_ok=True)


def _ensure_directory(path: Path, destination: Path) -> None:
    try:
        relative = path.relative_to(destination)
    except ValueError as error:
        raise GoldMaterializationError("unsafe_destination", "publication path escapes trusted destination") from error
    current = destination
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise GoldMaterializationError("unsafe_destination", "publication path traverses a symlink")
        if current.exists():
            if not current.is_dir():
                raise GoldMaterializationError("unsafe_destination", "publication path is not a directory")
        else:
            try:
                current.mkdir()
            except FileExistsError:
                if current.is_symlink() or not current.is_dir():
                    raise GoldMaterializationError("unsafe_destination", "publication path changed while creating it")


def _configure_duckdb(connection: duckdb.DuckDBPyConnection, temporary: Path) -> None:
    if temporary.exists():
        if temporary.is_symlink() or not temporary.is_dir():
            raise GoldMaterializationError("unsafe_destination", "DuckDB temporary path is unsafe")
    else:
        temporary.mkdir()
    connection.execute(f"SET memory_limit = {_sql_literal(DUCKDB_MEMORY_LIMIT)}")
    connection.execute(f"SET temp_directory = {_sql_literal(str(temporary))}")
    connection.execute("SET threads = 1")
    connection.execute("SET preserve_insertion_order = false")


def _physical_types(dataset: Mapping[str, Any]) -> dict[str, str]:
    try:
        return {
            field["name"]: "INTEGER" if field["name"] in {"hour", "input_hour_count"} else DUCKDB_TYPES[field["type"]]
            for field in dataset["schema"]
        }
    except (KeyError, TypeError) as error:
        raise GoldMaterializationError("catalog_contract_failure", "dataset schema has unsupported physical types") from error


def _empty_relation(dataset: Mapping[str, Any]) -> str:
    fields = dataset["schema"]
    projection = ", ".join(f"CAST(NULL AS {DUCKDB_TYPES[field['type']]}) AS {_sql_identifier(field['name'])}" for field in fields)
    return f"SELECT {projection} WHERE false"


def _field_projection(fields: Sequence[str], source: str) -> str:
    return ", ".join(f"{_sql_identifier(field)}" for field in fields)


def _read_parquet_sql(paths: Sequence[Path]) -> str:
    return f"read_parquet([{', '.join(_sql_literal(str(path)) for path in paths)}], hive_partitioning = false)"


def _join_columns(value: str) -> list[tuple[str, str]]:
    return [tuple(reference.split('.', 1)) for reference in value.split(',')]


def _kpi_formula(contract: _Contract, identifier: str) -> str:
    kpi = next((item for item in contract.metadata["kpis"] if item["id"] == identifier), None)
    if not isinstance(kpi, Mapping) or not isinstance(kpi.get("formula"), str):
        raise GoldMaterializationError("catalog_contract_failure", "required KPI is absent")
    return kpi["formula"]


def _partition_date(document: Mapping[str, Any], code: str) -> str:
    value = document.get("partition_date")
    if not isinstance(value, str):
        raise GoldMaterializationError(code, "partition_date must be YYYY-MM-DD")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise GoldMaterializationError(code, "partition_date must be YYYY-MM-DD") from error
    if parsed.isoformat() != value:
        raise GoldMaterializationError(code, "partition_date must be YYYY-MM-DD")
    return value


def _capture_label(partition_date: str, hour: int) -> str:
    moment = datetime.combine(date.fromisoformat(partition_date), datetime.min.time(), tzinfo=UTC)
    return (moment + timedelta(hours=hour + 1)).strftime("%Y%m%d-%H0000")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as error:
        raise GoldMaterializationError("input_read_failure", "cannot read governed input") from error
    return digest.hexdigest()


def _integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _require_clock(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise GoldMaterializationError("invalid_clock", "clock must be timezone-aware")
    return value.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    return _require_clock(value).isoformat(timespec="seconds").replace("+00:00", "Z")


def _default_run_id(now_value: datetime) -> str:
    return f"run-{_timestamp(now_value).replace(':', '').replace('-', '')}"


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def main() -> None:
    """Materialize one accepted daily Silver manifest into governed Gold metrics."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-manifest", required=True, type=Path)
    parser.add_argument("--destination", required=True, type=Path, help="trusted local publication root")
    parser.add_argument("--run-id", help="optional immutable publication identity")
    arguments = parser.parse_args()
    print(materialize_project_traffic_daily(arguments.silver_manifest, arguments.destination, run_id=arguments.run_id))


if __name__ == "__main__":
    main()
