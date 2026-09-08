"""Bind trusted accepted manifests to generated SQL and attributable results."""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from agent.query_safety import BoundView, QueryLimits, QuerySafetyError, SqlExecutor, validate_sql
from data.samples.wikimedia_fixtures import FixtureValidationError
from pipelines.batch import gold


class QueryAdapterError(ValueError):
    """A sanitized data, policy, or execution failure; never a partial result."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"[{code}] governed query could not complete")


@dataclass(frozen=True)
class SourceEvidence:
    dataset_id: str
    manifest_id: str
    manifest_path: str
    manifest_sha256: str
    partition_date: str


@dataclass(frozen=True)
class ResourcePolicyOutcome:
    outcome: str
    limits: QueryLimits
    scan_bytes_upper_bound: int


@dataclass(frozen=True)
class GovernedSqlResult:
    sql: str
    views: tuple[str, ...]
    columns: tuple[str, ...]
    rows: list[tuple[Any, ...]]
    row_count: int
    source_datasets: tuple[str, ...]
    sources: tuple[SourceEvidence, ...]
    data_status: str
    elapsed_seconds: float
    execution_seconds: float
    resource_policy: ResourcePolicyOutcome


class GovernedSqlAdapter:
    """Service-owned local adapter; only execute's SQL may originate from a model.

    The host selects one immutable Gold or fixture-freshness manifest and optional
    required partition. Existing Gold validators are the lineage authority.
    """

    def __init__(
        self,
        manifest: Path,
        destination: Path,
        *,
        catalog_path: Path = gold.CATALOG_PATH,
        limits: QueryLimits = QueryLimits(),
        required_partition_date: str | None = None,
    ) -> None:
        if required_partition_date is not None:
            if date.fromisoformat(required_partition_date).isoformat() != required_partition_date:
                raise ValueError("required_partition_date must be YYYY-MM-DD")
        try:
            self._contract = gold._load_contract(catalog_path)
        except gold.GoldMaterializationError as error:
            raise QueryAdapterError(error.code) from error
        self._manifest = manifest.absolute()
        self._destination = destination.absolute()
        self._required_partition = required_partition_date
        self._limits = limits
        self._executor = SqlExecutor(frozenset(self._contract.views), limits=limits)

    def execute(self, sql: str) -> GovernedSqlResult:
        """Revalidate immutable evidence on each call, then execute bounded SQL."""

        started = time.monotonic()
        try:
            validate_sql(sql, frozenset(self._contract.views))
            bindings, sources, data_status = self._bind()
            result = self._executor.execute(sql, bindings)
        except (gold.GoldMaterializationError, gold.GovernedQueryError, QuerySafetyError) as error:
            raise QueryAdapterError(error.code) from error
        except FixtureValidationError as error:
            raise QueryAdapterError("invalid_freshness_manifest") from error
        except OSError as error:
            raise QueryAdapterError("input_read_failure") from error
        return GovernedSqlResult(
            result.sql, result.views, result.columns, result.rows, len(result.rows),
            tuple(source.dataset_id for source in sources), sources, data_status,
            time.monotonic() - started, result.elapsed_seconds,
            ResourcePolicyOutcome("passed", self._limits, result.scan_bytes_upper_bound),
        )

    def _bind(self) -> tuple[dict[str, BoundView], tuple[SourceEvidence, ...], str]:
        root = self._destination
        # Validate ancestors without creating a query destination or temporary files.
        for candidate in (root, *root.parents):
            if candidate.is_symlink():
                raise QueryAdapterError("unsafe_destination")
        try:
            relative = self._manifest.relative_to(root)
        except ValueError as error:
            raise QueryAdapterError("unsafe_input") from error
        if ".." in relative.parts:
            raise QueryAdapterError("unsafe_input")
        current = root
        for part in relative.parts:
            current = current / part
            if current.is_symlink():
                raise QueryAdapterError("unsafe_input")
        if not self._manifest.exists():
            raise QueryAdapterError("missing_data")
        raw, document, relative = gold._read_canonical_manifest(self._manifest, root, "unsafe_input")
        if document.get("schema") == gold.FRESHNESS_MANIFEST_SCHEMA:
            traffic, silver = None, None
            freshness = gold._load_accepted_freshness(self._manifest, root, self._contract)
            evidence = document["fixture_evidence"]
            status = "complete" if evidence["accepted_count"] == evidence["expected_count"] else "missing"
            bound_path = freshness
        else:
            traffic, silver = gold._load_accepted_gold(self._manifest, root, self._contract)
            freshness = None
            status = "complete"
            bound_path = traffic
        partition = document["partition_date"]
        if self._required_partition is not None and partition != self._required_partition:
            raise QueryAdapterError("stale_data")
        sources = [SourceEvidence(
            document["dataset_id"], document["run"]["run_id"], relative.as_posix(),
            hashlib.sha256(raw).hexdigest(), partition,
        )]
        if silver is not None:
            sources.append(SourceEvidence(
                "pageviews_hourly", silver.manifest_id, silver.manifest_relative,
                silver.manifest_sha256, silver.partition_date,
            ))
        queries = gold._catalog_view_queries(self._contract, traffic, silver, freshness)
        bindings = {name: BoundView(query, bound_path.stat().st_size) for name, query in queries.items()}
        return bindings, tuple(sources), status
