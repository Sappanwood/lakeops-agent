"""Validate and publish the repository's bounded Wikimedia fixture bundles."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping


FIXTURE_ROOT = Path(__file__).resolve().parent / "wikimedia"
CATALOG_PATH = Path(__file__).resolve().parents[2] / "data" / "catalog" / "catalog.json"
FORBIDDEN_STREAM_FIELDS = frozenset({"user", "comment", "parsedcomment", "log_params", "ip"})
DERIVED_STREAM_FIELDS = frozenset({"partition_date", "hour"})


class FixtureValidationError(ValueError):
    """Raised when a committed fixture cannot form a safe, complete bundle."""


def canonical_json(value: object) -> str:
    """Return the one serialization used for checksums and comparisons."""

    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def load_fixture_profiles(source_root: Path = FIXTURE_ROOT) -> dict[str, dict[str, Any]]:
    """Load the bounded local fixture profiles and reject malformed limits."""

    document = _read_json(source_root / "profiles.json")
    profiles = document.get("profiles")
    if not isinstance(profiles, dict) or set(profiles) != {"tiny", "demo"}:
        raise FixtureValidationError("profiles must define exactly tiny and demo")
    for name, profile in profiles.items():
        if not isinstance(profile, dict):
            raise FixtureValidationError(f"profile {name!r} must be an object")
        for key in ("max_source_bytes", "max_records", "max_runtime_seconds", "pageview_hours"):
            if not isinstance(profile.get(key), int) or profile[key] <= 0:
                raise FixtureValidationError(f"profile {name!r} has invalid {key!r}")
        capture_ends = profile.get("capture_ends")
        if not isinstance(capture_ends, list) or len(capture_ends) != profile["pageview_hours"]:
            raise FixtureValidationError(f"profile {name!r} must select one fixed capture end per pageview hour")
        if len(set(capture_ends)) != len(capture_ends) or not all(isinstance(value, str) for value in capture_ends):
            raise FixtureValidationError(f"profile {name!r} has invalid fixed capture ends")
    return profiles


def build_fixture_manifest(
    scenario: str, source_root: Path = FIXTURE_ROOT, *, profile: str | None = None
) -> dict[str, Any]:
    """Build a canonical fixture manifest from committed, small source excerpts."""

    if scenario not in {"complete_day", "missing_hour_day"}:
        raise FixtureValidationError(f"unknown fixture scenario {scenario!r}")
    descriptor = _read_json(source_root / "scenarios.json")[scenario]
    if not isinstance(descriptor, dict):
        raise FixtureValidationError(f"scenario {scenario!r} must be an object")
    partition_date = descriptor.get("partition_date")
    if not isinstance(partition_date, str):
        raise FixtureValidationError(f"scenario {scenario!r} has no partition_date")
    source_objects = _load_pageview_objects(
        source_root / "pageviews" / descriptor["pageview_fixture"], source_root
    )
    replay = _load_replay(source_root / "recentchange" / "replay.json")
    expected_capture_ends = _expected_capture_ends(partition_date)
    seen_capture_ends = {item["capture_end"] for item in source_objects}
    missing = sorted(expected_capture_ends - seen_capture_ends)
    if descriptor["expected_source_object_count"] != len(source_objects):
        raise FixtureValidationError(f"scenario {scenario!r} has an unexpected source-object count")
    if descriptor["expected_missing_capture_end"] != missing:
        raise FixtureValidationError(f"scenario {scenario!r} has unexpected missing capture ends")
    profiles = load_fixture_profiles(source_root)
    manifest: dict[str, Any] = {
        "schema": "lakeops/fixture-manifest@1",
        "scenario": scenario,
        "partition_date": partition_date,
        "source_id": "wikimedia_pageviews",
        "source_objects": source_objects,
        "missing_capture_end": missing,
        "recentchange_replay": replay,
        "profiles": profiles,
    }
    _assert_committed_manifest(manifest, source_root / "manifests" / f"{scenario}.json")
    if profile is not None:
        if profile not in profiles:
            raise FixtureValidationError(f"unknown fixture profile {profile!r}")
        selected_capture_ends = profiles[profile]["capture_ends"]
        selected = [item for item in source_objects if item["capture_end"] in selected_capture_ends]
        if len(selected) != len(selected_capture_ends):
            raise FixtureValidationError(f"profile {profile!r} selects source hours absent from {scenario!r}")
        manifest["source_objects"] = selected
        manifest["profile"] = profile
        manifest["fixture_measurements"] = _fixture_measurements(selected)
    return manifest


def canonical_records(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return source records in a dependency-independent deterministic order."""

    records: list[dict[str, Any]] = []
    for source_object in manifest["source_objects"]:
        capture_end = source_object["capture_end"]
        for record in source_object["records"]:
            records.append({"capture_end": capture_end, **record})
    return sorted(records, key=lambda item: (item["capture_end"], item["domain_code"], item["page_title"]))


def project_recentchange_event(event: Mapping[str, Any]) -> dict[str, Any]:
    """Project an in-memory EventStreams event before it reaches fixture storage."""

    if not isinstance(event, Mapping):
        raise FixtureValidationError("recent-change event must be an object")
    _reject_forbidden_nested_keys(event)
    contract = _stream_field_contract()
    unexpected = sorted(set(event) - set(contract))
    if unexpected:
        raise FixtureValidationError(f"recent-change event has fields outside the projected contract {unexpected}")
    missing = sorted(field for field, definition in contract.items() if not definition["nullable"] and field not in event)
    if missing:
        raise FixtureValidationError(f"recent-change event is missing required fields {missing}")
    projected: dict[str, Any] = {}
    for field, definition in contract.items():
        if field not in event:
            continue
        value = event[field]
        _validate_stream_scalar(field, value, definition)
        projected[field] = value
    return projected


def publish_fixture_bundle(
    scenario: str,
    destination: Path,
    *,
    source_root: Path = FIXTURE_ROOT,
    profile: str = "demo",
    inject_projection_failure: bool = False,
    clock: Callable[[], float] = time.monotonic,
) -> Path:
    """Validate all fixture inputs, then atomically publish one canonical manifest."""

    started_at = clock()
    manifest = build_fixture_manifest(scenario, source_root, profile=profile)
    if manifest["missing_capture_end"]:
        raise FixtureValidationError("a missing-hour scenario cannot publish an accepted manifest")
    for replay_case in manifest["recentchange_replay"]:
        if replay_case["case"] == "invalid":
            try:
                project_recentchange_event(replay_case["event"])
            except FixtureValidationError:
                continue
            raise FixtureValidationError("invalid replay case unexpectedly passed projection")
        project_recentchange_event(replay_case["event"])
    if inject_projection_failure:
        raise FixtureValidationError("injected projection failure")
    _enforce_profile_limits(manifest, clock() - started_at)

    _prepare_destination(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="lakeops-fixture-", dir=destination.parent) as temporary_directory:
        staged_manifest = Path(temporary_directory) / "fixture-manifest.json"
        try:
            staged_manifest.write_text(canonical_json(manifest) + "\n", encoding="utf-8")
        except OSError as error:
            raise FixtureValidationError("cannot stage fixture manifest") from error
        destination.mkdir(parents=True, exist_ok=True)
        published = destination / "fixture-manifest.json"
        if published.exists() or published.is_symlink():
            raise FixtureValidationError("fixture manifest already exists; publication is no-clobber")
        try:
            os.link(staged_manifest, published)
        except FileExistsError as error:
            raise FixtureValidationError("fixture manifest already exists; publication is no-clobber") from error
        except OSError as error:
            raise FixtureValidationError("cannot atomically publish fixture manifest") from error
    return published


def _load_pageview_objects(path: Path, source_root: Path) -> list[dict[str, Any]]:
    document = _read_json(path)
    objects = document.get("source_objects")
    if not isinstance(objects, list) or not objects:
        raise FixtureValidationError(f"{path} must contain source_objects")
    canonical_objects: list[dict[str, Any]] = []
    capture_ends: set[str] = set()
    for item in objects:
        if not isinstance(item, dict):
            raise FixtureValidationError("pageview source object must be an object")
        capture_end = item.get("capture_end")
        records = item.get("records")
        if not isinstance(capture_end, str) or capture_end in capture_ends:
            raise FixtureValidationError("pageview source object has an invalid or duplicate capture_end")
        if not isinstance(records, list) or not records:
            raise FixtureValidationError("pageview source object must contain records")
        canonical_records = []
        for record in records:
            if not isinstance(record, dict) or set(record) != {"domain_code", "page_title", "count_views", "total_response_size"}:
                raise FixtureValidationError("pageview record does not match the source file schema")
            if not all(isinstance(record[key], str) and record[key] for key in record):
                raise FixtureValidationError("pageview fixture values must be non-empty source strings")
            canonical_records.append(dict(record))
        identity = {"capture_end": capture_end, "records": canonical_records}
        fixture_bytes = canonical_json(identity).encode("ascii")
        timestamp = datetime.fromisoformat(capture_end.replace("Z", "+00:00"))
        source_url = (
            "https://dumps.wikimedia.org/other/pageviews/"
            f"{timestamp:%Y}/{timestamp:%Y-%m}/pageviews-{timestamp:%Y%m%d-%H%M%S}.gz"
        )
        canonical_objects.append(
            {
                "capture_end": capture_end,
                "fixture_path": str(path.relative_to(source_root)),
                "fixture_sha256": hashlib.sha256(fixture_bytes).hexdigest(),
                "fixture_byte_count": len(fixture_bytes),
                "upstream_source_url": source_url,
                "upstream_source_sha256": None,
                "record_count": len(canonical_records),
                "records": canonical_records,
            }
        )
        capture_ends.add(capture_end)
    return sorted(canonical_objects, key=lambda item: item["capture_end"])


def _load_replay(path: Path) -> list[dict[str, Any]]:
    document = _read_json(path)
    cases = document.get("cases")
    expected = {"normal", "late", "invalid", "duplicate", "out_of_order"}
    if not isinstance(cases, list) or {item.get("case") for item in cases if isinstance(item, dict)} != expected:
        raise FixtureValidationError("recent-change replay must cover normal, late, invalid, duplicate, and out_of_order")
    replay: list[dict[str, Any]] = []
    for item in cases:
        event = item.get("event")
        if not isinstance(event, dict):
            raise FixtureValidationError("recent-change replay case must contain an event")
        leaked = FORBIDDEN_STREAM_FIELDS & set(event)
        if leaked:
            raise FixtureValidationError(f"recent-change fixture retains forbidden fields {sorted(leaked)}")
        replay.append({"case": item["case"], "event": dict(event)})
    return replay


def _stream_field_contract() -> dict[str, dict[str, Any]]:
    catalog = _read_json(CATALOG_PATH)
    datasets = catalog.get("datasets")
    if not isinstance(datasets, list):
        raise FixtureValidationError("catalog has no datasets")
    dataset = next((item for item in datasets if isinstance(item, dict) and item.get("id") == "recentchange_events"), None)
    if not isinstance(dataset, dict) or not isinstance(dataset.get("schema"), list):
        raise FixtureValidationError("catalog has no recentchange_events schema")
    contract: dict[str, dict[str, Any]] = {}
    for definition in dataset["schema"]:
        if not isinstance(definition, dict):
            raise FixtureValidationError("recentchange_events schema contains an invalid field")
        name = definition.get("name")
        field_type = definition.get("type")
        nullable = definition.get("nullable")
        if not isinstance(name, str) or not isinstance(field_type, str) or not isinstance(nullable, bool):
            raise FixtureValidationError("recentchange_events schema has an invalid scalar contract")
        if name in DERIVED_STREAM_FIELDS:
            continue
        contract[name] = {"type": field_type, "nullable": nullable}
    return contract


def _reject_forbidden_nested_keys(value: object) -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if key in FORBIDDEN_STREAM_FIELDS:
                raise FixtureValidationError(f"recent-change event retains forbidden field {key!r}")
            _reject_forbidden_nested_keys(nested)
    elif isinstance(value, (list, tuple)):
        for nested in value:
            _reject_forbidden_nested_keys(nested)


def _validate_stream_scalar(field: str, value: Any, definition: Mapping[str, Any]) -> None:
    if value is None:
        if definition["nullable"]:
            return
        raise FixtureValidationError(f"recent-change field {field!r} is not nullable")
    field_type = definition["type"]
    valid = (
        (field_type == "string" and isinstance(value, str))
        or (field_type == "integer" and type(value) is int)
        or (field_type == "boolean" and type(value) is bool)
        or (field_type == "timestamp" and isinstance(value, str))
    )
    if not valid:
        raise FixtureValidationError(f"recent-change field {field!r} has invalid {field_type!r} scalar form")
    if field_type == "timestamp":
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise FixtureValidationError(f"recent-change field {field!r} has an invalid timestamp") from error


def _fixture_measurements(source_objects: list[Mapping[str, Any]]) -> dict[str, int]:
    return {
        "source_bytes": sum(item["fixture_byte_count"] for item in source_objects),
        "record_count": sum(item["record_count"] for item in source_objects),
        "pageview_hours": len(source_objects),
    }


def _enforce_profile_limits(manifest: Mapping[str, Any], elapsed_seconds: float) -> None:
    profile_name = manifest.get("profile")
    profiles = manifest.get("profiles")
    measurements = manifest.get("fixture_measurements")
    if not isinstance(profile_name, str) or not isinstance(profiles, Mapping) or not isinstance(measurements, Mapping):
        raise FixtureValidationError("published fixture manifest must select an executable profile")
    profile = profiles.get(profile_name)
    if not isinstance(profile, Mapping):
        raise FixtureValidationError("published fixture profile is invalid")
    for measured_key, limit_key in (("source_bytes", "max_source_bytes"), ("record_count", "max_records"), ("pageview_hours", "pageview_hours")):
        if measurements[measured_key] > profile[limit_key]:
            raise FixtureValidationError(f"fixture profile {profile_name!r} exceeds {limit_key}")
    if elapsed_seconds > profile["max_runtime_seconds"]:
        raise FixtureValidationError(f"fixture profile {profile_name!r} exceeds max_runtime_seconds")


def _prepare_destination(destination: Path) -> None:
    for candidate in (destination, destination.parent, *destination.parent.parents):
        if candidate.is_symlink():
            raise FixtureValidationError("fixture destination must not traverse an existing symlink")
    if destination.exists() and not destination.is_dir():
        raise FixtureValidationError("fixture destination must be a directory")


def _validate_committed_fixture_provenance(manifest: Mapping[str, Any]) -> None:
    source_objects = manifest.get("source_objects")
    if not isinstance(source_objects, list):
        raise FixtureValidationError("committed manifest has no source objects")
    required = {
        "capture_end",
        "fixture_path",
        "fixture_sha256",
        "fixture_byte_count",
        "upstream_source_url",
        "upstream_source_sha256",
        "record_count",
        "records",
    }
    for item in source_objects:
        if not isinstance(item, dict) or set(item) != required:
            raise FixtureValidationError("committed manifest has mixed fixture and upstream provenance")
        checksum = item["fixture_sha256"]
        if not isinstance(checksum, str) or len(checksum) != 64 or any(character not in "0123456789abcdef" for character in checksum):
            raise FixtureValidationError("committed manifest has an invalid fixture checksum")
        if item["upstream_source_sha256"] is not None:
            raise FixtureValidationError("committed source excerpts must not claim unavailable upstream checksums")
        if not isinstance(item["upstream_source_url"], str) or not item["upstream_source_url"].startswith("https://dumps.wikimedia.org/"):
            raise FixtureValidationError("committed manifest has an invalid upstream source URL")


def _expected_capture_ends(partition_date: str) -> set[str]:
    start = datetime.fromisoformat(f"{partition_date}T00:00:00+00:00")
    return {(start + timedelta(hours=offset)).isoformat().replace("+00:00", "Z") for offset in range(1, 25)}


def _assert_committed_manifest(manifest: Mapping[str, Any], path: Path) -> None:
    if not path.exists():
        return
    committed = _read_json(path)
    _validate_committed_fixture_provenance(committed)
    if canonical_json(committed) != canonical_json(_pinned_manifest(manifest)):
        raise FixtureValidationError(f"committed manifest {path.name!r} does not match fixture inputs")


def _pinned_manifest(manifest: Mapping[str, Any]) -> dict[str, Any]:
    pinned = dict(manifest)
    pinned.pop("profiles", None)
    pinned.pop("profile", None)
    pinned.pop("fixture_measurements", None)
    return pinned


def _read_json(path: Path) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise FixtureValidationError(f"cannot read fixture document {path}") from error
    if not isinstance(document, dict):
        raise FixtureValidationError(f"fixture document {path} must be an object")
    return document
