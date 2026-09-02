"""Download and immutably publish Wikimedia Pageviews bronze source objects."""

from __future__ import annotations

import argparse
import gzip
import hashlib
from http.client import IncompleteRead
import json
import os
import re
import shutil
import tempfile
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import BinaryIO, Protocol
from urllib.request import Request, urlopen


PAGEVIEWS_BASE_URL = "https://dumps.wikimedia.org/other/pageviews"
PROFILE_HOURS = {"tiny": 1, "demo": 6, "daily": 24}
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


class BronzeIngestionError(ValueError):
    """An inspectable bronze ingestion failure with a stable error code."""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"[{code}] {detail}")


class DownloadResponse(Protocol):
    """The minimal streaming HTTP response contract used by the downloader."""

    status: int
    headers: Mapping[str, str]

    def read(self, size: int = -1) -> bytes: ...

    def __enter__(self) -> BinaryIO: ...

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> object: ...


@dataclass(frozen=True)
class SourcePartition:
    """One exact upstream source identity for an hourly pageviews capture."""

    capture_end: str
    source_url: str


def expected_source_partitions(partition_date: str, profile: str) -> list[SourcePartition]:
    """Return the exact capture-end URLs selected by one local execution profile."""

    if profile not in PROFILE_HOURS:
        raise BronzeIngestionError("invalid_profile", f"unsupported profile {profile!r}")
    try:
        day = date.fromisoformat(partition_date)
    except ValueError as error:
        raise BronzeIngestionError("invalid_partition_date", "partition_date must be YYYY-MM-DD") from error
    if day.isoformat() != partition_date:
        raise BronzeIngestionError("invalid_partition_date", "partition_date must be YYYY-MM-DD")

    start = datetime.combine(day, datetime.min.time(), tzinfo=UTC)
    partitions: list[SourcePartition] = []
    for hour_offset in range(1, PROFILE_HOURS[profile] + 1):
        capture_end = start + timedelta(hours=hour_offset)
        partitions.append(
            SourcePartition(
                capture_end=_timestamp(capture_end),
                source_url=(
                    f"{PAGEVIEWS_BASE_URL}/{capture_end:%Y}/{capture_end:%Y-%m}/"
                    f"pageviews-{capture_end:%Y%m%d-%H%M%S}.gz"
                ),
            )
        )
    return partitions


def ingest_pageviews(
    partition_date: str,
    profile: str,
    destination: Path,
    *,
    downloader: Callable[[str], DownloadResponse] | None = None,
    source_partitions: Sequence[SourcePartition] | None = None,
    run_id: str | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Path:
    """Download, validate, and publish one immutable pageviews bronze manifest.

    The accepted filesystem boundary is a trusted local destination. Existing
    symlinks are rejected during the static containment check, and final files
    use POSIX hard-link publication for normal-concurrency no-clobber behavior.
    """

    expected = expected_source_partitions(partition_date, profile)
    selected = list(source_partitions) if source_partitions is not None else expected
    _validate_selected_partitions(selected, expected)
    run_started = now()
    if run_started.tzinfo is None:
        raise BronzeIngestionError("invalid_clock", "now() must return a timezone-aware timestamp")
    identifier = run_id or _default_run_id(run_started)
    if not RUN_ID_PATTERN.fullmatch(identifier):
        raise BronzeIngestionError("invalid_run_id", "run_id must contain only letters, digits, '.', '_' or '-'")

    destination = destination.absolute()
    _prepare_destination(destination)
    active_downloader = downloader or _http_downloader
    started_at = _timestamp(run_started)

    try:
        with tempfile.TemporaryDirectory(prefix="lakeops-bronze-", dir=destination) as temporary_directory:
            staging = Path(temporary_directory)
            source_objects = _stage_sources(selected, staging, active_downloader, now)
            manifest = _build_manifest(partition_date, profile, identifier, started_at, source_objects, now())
            staged_manifest = staging / "manifest.json"
            staged_manifest.write_text(_canonical_json(manifest) + "\n", encoding="utf-8")
            return _publish(staging, destination, identifier, source_objects, staged_manifest)
    except BronzeIngestionError:
        raise
    except OSError as error:
        raise BronzeIngestionError("local_io_failure", str(error)) from error


def _stage_sources(
    source_partitions: Iterable[SourcePartition],
    staging: Path,
    downloader: Callable[[str], DownloadResponse],
    now: Callable[[], datetime],
) -> list[dict[str, object]]:
    source_objects: list[dict[str, object]] = []
    for index, partition in enumerate(source_partitions):
        capture_end = _parse_timestamp(partition.capture_end, "invalid_source_partition")
        filename = f"pageviews-{capture_end:%Y%m%d-%H%M%S}.gz"
        staged_object = staging / filename
        metadata = _download_to_staging(partition.source_url, staged_object, downloader, now)
        record_count = _validate_pageviews_gzip(staged_object)
        logical_hour = capture_end - timedelta(hours=1)
        source_objects.append(
            {
                "capture_end": _timestamp(capture_end),
                "logical_hour": _timestamp(logical_hour),
                "source_url": partition.source_url,
                "source_last_modified": metadata["source_last_modified"],
                "source_etag": metadata["source_etag"],
                "source_content_length": metadata["source_content_length"],
                "source_sha256": metadata["source_sha256"],
                "retrieved_at": metadata["retrieved_at"],
                "downloaded_byte_count": metadata["source_content_length"],
                "record_count": record_count,
                "staged_filename": filename,
                "source_index": index,
            }
        )
    return source_objects


def _download_to_staging(
    source_url: str,
    staged_object: Path,
    downloader: Callable[[str], DownloadResponse],
    now: Callable[[], datetime],
) -> dict[str, object]:
    try:
        response = downloader(source_url)
    except OSError as error:
        raise BronzeIngestionError("source_unavailable", f"cannot fetch {source_url}: {error}") from error
    try:
        with response:
            if response.status != 200:
                raise BronzeIngestionError("source_http_status", f"{source_url} returned HTTP {response.status}")
            content_length = _required_content_length(response.headers, source_url)
            source_etag = _required_header(response.headers, "ETag", source_url)
            source_last_modified = _required_header(response.headers, "Last-Modified", source_url)
            digest, downloaded = _stage_response_body(response, staged_object, source_url)
    except BronzeIngestionError:
        raise
    except OSError as error:
        raise BronzeIngestionError("source_read_failure", f"cannot read {source_url}: {error}") from error
    if downloaded != content_length:
        raise BronzeIngestionError(
            "conflicting_source_metadata",
            f"{source_url} declared Content-Length {content_length} but delivered {downloaded} bytes",
        )
    return {
        "source_last_modified": source_last_modified,
        "source_etag": source_etag,
        "source_content_length": content_length,
        "source_sha256": digest.hexdigest(),
        "retrieved_at": _timestamp(now()),
    }


def _stage_response_body(response: DownloadResponse, staged_object: Path, source_url: str) -> tuple[hashlib._Hash, int]:
    digest = hashlib.sha256()
    downloaded = 0
    try:
        with staged_object.open("xb") as output:
            while True:
                try:
                    chunk = response.read(1024 * 1024)
                except (IncompleteRead, OSError) as error:
                    raise BronzeIngestionError("source_read_failure", f"cannot read {source_url}: {error}") from error
                if not chunk:
                    break
                if not isinstance(chunk, bytes):
                    raise BronzeIngestionError("invalid_source_body", f"{source_url} returned a non-bytes response chunk")
                output.write(chunk)
                digest.update(chunk)
                downloaded += len(chunk)
    except BronzeIngestionError:
        raise
    except OSError as error:
        raise BronzeIngestionError("staging_write_failure", f"cannot stage {source_url}: {error}") from error
    return digest, downloaded


def _validate_pageviews_gzip(path: Path) -> int:
    records = 0
    try:
        with gzip.open(path, mode="rt", encoding="utf-8", newline="") as source:
            for line_number, raw_line in enumerate(source, start=1):
                if not raw_line.endswith("\n"):
                    raise BronzeIngestionError("malformed_source_content", f"{path.name}:{line_number} has no newline")
                line = raw_line[:-1]
                fields = line.split(" ")
                if len(fields) != 4 or any(not field for field in fields):
                    raise BronzeIngestionError("malformed_source_content", f"{path.name}:{line_number} must have four fields")
                domain_code, page_title, count_views, total_response_size = fields
                if not domain_code or not page_title or not count_views.isdecimal() or not total_response_size.isdecimal():
                    raise BronzeIngestionError("malformed_source_content", f"{path.name}:{line_number} violates the pageviews schema")
                records += 1
    except BronzeIngestionError:
        raise
    except (EOFError, OSError, UnicodeDecodeError) as error:
        raise BronzeIngestionError("invalid_gzip_source", f"cannot validate {path.name}: {error}") from error
    if records == 0:
        raise BronzeIngestionError("malformed_source_content", f"{path.name} has no pageviews records")
    return records


def _build_manifest(
    partition_date: str,
    profile: str,
    run_id: str,
    started_at: str,
    source_objects: Sequence[dict[str, object]],
    finished_at: datetime,
) -> dict[str, object]:
    published_objects: list[dict[str, object]] = []
    for source in source_objects:
        filename = str(source["staged_filename"])
        logical_hour = _parse_timestamp(str(source["logical_hour"]), "invalid_source_partition")
        object_path = (
            f"bronze/pageviews/partition_date={logical_hour:%Y-%m-%d}/hour={logical_hour.hour:02d}/"
            f"run_id={run_id}/{filename}"
        )
        published_objects.append({key: value for key, value in source.items() if key not in {"staged_filename", "source_index"}} | {"object_path": object_path})
    return {
        "schema": "lakeops/bronze-pageviews-manifest@1",
        "manifest_id": run_id,
        "status": "accepted",
        "source_id": "wikimedia_pageviews",
        "partition_date": partition_date,
        "profile": profile,
        "run": {
            "run_id": run_id,
            "started_at": started_at,
            "finished_at": _timestamp(finished_at),
            "input_object_count": len(published_objects),
        },
        "source_objects": published_objects,
    }


def _publish(
    staging: Path,
    destination: Path,
    run_id: str,
    source_objects: Sequence[dict[str, object]],
    staged_manifest: Path,
) -> Path:
    logical_hours = [_parse_timestamp(str(source["logical_hour"]), "invalid_source_partition") for source in source_objects]
    logical_dates = {logical_hour.date().isoformat() for logical_hour in logical_hours}
    if len(logical_dates) != 1:
        raise BronzeIngestionError("conflicting_source_partitions", "source objects span multiple logical dates")
    logical_partition_date = logical_dates.pop()
    bronze_parent = destination / "bronze" / "pageviews" / f"partition_date={logical_partition_date}"
    _ensure_directory(bronze_parent, destination)
    run_claim_parent = bronze_parent / ".runs"
    _ensure_directory(run_claim_parent, destination)
    run_claim = run_claim_parent / f"run_id={run_id}"

    claimed_run = False
    published_run_directories: list[Path] = []
    try:
        try:
            run_claim.mkdir()
            claimed_run = True
        except FileExistsError as error:
            raise BronzeIngestionError("publication_conflict", f"bronze run already exists: {run_claim}") from error
        for source in source_objects:
            staged_object = staging / str(source["staged_filename"])
            logical_hour = _parse_timestamp(str(source["logical_hour"]), "invalid_source_partition")
            hour_parent = bronze_parent / f"hour={logical_hour.hour:02d}"
            _ensure_directory(hour_parent, destination)
            final_run = hour_parent / f"run_id={run_id}"
            try:
                final_run.mkdir()
            except FileExistsError as error:
                raise BronzeIngestionError(
                    "publication_conflict",
                    f"bronze run directory already exists: {final_run}",
                ) from error
            except OSError as error:
                raise BronzeIngestionError(
                    "publication_failure",
                    f"cannot create bronze run directory: {final_run}",
                ) from error
            published_run_directories.append(final_run)
            try:
                os.link(staged_object, final_run / staged_object.name)
            except FileExistsError as error:
                raise BronzeIngestionError("publication_conflict", f"bronze object already exists: {staged_object.name}") from error
            except OSError as error:
                raise BronzeIngestionError("publication_failure", f"cannot publish bronze object {staged_object.name}: {error}") from error
        manifest_parent = destination / "manifests" / "pageviews_hourly" / f"partition_date={logical_partition_date}"
        _ensure_directory(manifest_parent, destination)
        final_manifest = manifest_parent / f"{run_id}.json"
        if final_manifest.exists() or final_manifest.is_symlink():
            raise BronzeIngestionError("publication_conflict", f"manifest already exists: {final_manifest}")
        try:
            os.link(staged_manifest, final_manifest)
        except FileExistsError as error:
            raise BronzeIngestionError("publication_conflict", f"manifest already exists: {final_manifest}") from error
        except OSError as error:
            raise BronzeIngestionError("publication_failure", f"cannot publish manifest: {error}") from error
    except BronzeIngestionError:
        for final_run in reversed(published_run_directories):
            _remove_owned_directory(final_run)
        if claimed_run:
            _remove_owned_directory(run_claim)
        raise
    return final_manifest


def _validate_selected_partitions(selected: Sequence[SourcePartition], expected: Sequence[SourcePartition]) -> None:
    if len(selected) != len(expected):
        raise BronzeIngestionError("incomplete_source_partitions", "selected partitions do not match the required profile")
    if list(selected) != list(expected):
        raise BronzeIngestionError("conflicting_source_partitions", "selected partitions are not the pinned expected source identities")


def _prepare_destination(destination: Path) -> None:
    for candidate in (destination, *destination.parents):
        if candidate.is_symlink():
            raise BronzeIngestionError("unsafe_destination", f"destination traverses symlink {candidate}")
    if destination.exists() and not destination.is_dir():
        raise BronzeIngestionError("unsafe_destination", f"destination is not a directory: {destination}")
    destination.mkdir(parents=True, exist_ok=True)


def _ensure_directory(path: Path, destination: Path) -> None:
    try:
        relative = path.relative_to(destination)
    except ValueError as error:
        raise BronzeIngestionError("unsafe_destination", f"path escapes destination: {path}") from error
    current = destination
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise BronzeIngestionError("unsafe_destination", f"destination traverses symlink {current}")
        if current.exists():
            if not current.is_dir():
                raise BronzeIngestionError("unsafe_destination", f"publication path is not a directory: {current}")
            continue
        try:
            current.mkdir()
        except FileExistsError:
            if current.is_symlink() or not current.is_dir():
                raise BronzeIngestionError("unsafe_destination", f"publication path changed while creating {current}")


def _remove_owned_directory(path: Path) -> None:
    if path.is_symlink():
        raise BronzeIngestionError("unsafe_destination", f"refusing to clean symlink {path}")
    try:
        shutil.rmtree(path)
    except OSError as error:
        raise BronzeIngestionError("publication_cleanup_failure", f"cannot clean incomplete run {path}: {error}") from error


def _http_downloader(source_url: str) -> DownloadResponse:
    request = Request(source_url, headers={"User-Agent": "lakeops-agent/0.1 bronze-ingestion"})
    try:
        return urlopen(request, timeout=60)
    except OSError as error:
        raise BronzeIngestionError("source_unavailable", f"cannot fetch {source_url}: {error}") from error


def _required_content_length(headers: Mapping[str, str], source_url: str) -> int:
    value = _required_header(headers, "Content-Length", source_url)
    try:
        content_length = int(value)
    except ValueError as error:
        raise BronzeIngestionError("invalid_source_metadata", f"{source_url} has invalid Content-Length {value!r}") from error
    if content_length <= 0:
        raise BronzeIngestionError("invalid_source_metadata", f"{source_url} has non-positive Content-Length")
    return content_length


def _required_header(headers: Mapping[str, str], name: str, source_url: str) -> str:
    value = headers.get(name)
    if not isinstance(value, str) or not value.strip():
        raise BronzeIngestionError("missing_source_metadata", f"{source_url} has no {name} header")
    return value


def _parse_timestamp(value: str, code: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise BronzeIngestionError(code, f"invalid UTC timestamp {value!r}") from error
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise BronzeIngestionError(code, f"timestamp must be UTC: {value!r}")
    return parsed


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise BronzeIngestionError("invalid_clock", "timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _default_run_id(now_value: datetime) -> str:
    return f"run-{_timestamp(now_value).replace(':', '').replace('-', '')}"


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def main() -> None:
    """Run one live, pinned Pageviews bronze ingestion from the command line."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--partition-date", required=True, help="UTC logical date in YYYY-MM-DD form")
    parser.add_argument("--profile", required=True, choices=sorted(PROFILE_HOURS))
    parser.add_argument("--destination", required=True, type=Path, help="trusted local publication root")
    parser.add_argument("--run-id", help="optional immutable publication identity")
    arguments = parser.parse_args()
    manifest = ingest_pageviews(
        arguments.partition_date,
        arguments.profile,
        arguments.destination,
        run_id=arguments.run_id,
    )
    print(manifest)


if __name__ == "__main__":
    main()
