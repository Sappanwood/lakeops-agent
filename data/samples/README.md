# Sample data

This directory is reserved for small deterministic Wikimedia fixtures that are
safe for a public repository. Fixtures must be minimal projections of the public
source contracts, include provenance, and exclude recent-change identity and
free-text fields.

Upstream pageview dumps, live stream captures, generated Parquet, and volume-test
data belong under `data/generated/` and are ignored by Git. Bulk source data must
be reproducible from a manifest and is never committed.

## Wikimedia fixtures

`wikimedia/` contains source excerpts, replay cases, profiles, and canonical
manifests. `complete_day` pins all 24 capture-end source labels for 2024-01-01;
`missing_hour_day` deliberately withholds the `2024-01-01T15:00:00Z` label.
The source URLs are official dump identities, while checksums cover the small
committed excerpts rather than upstream bulk objects. Each source object records
`fixture_sha256` and `fixture_path`; `upstream_source_sha256` is explicitly
`null` because these excerpts are not downloaded upstream objects and no upstream
checksum is fabricated. This fixture-specific provenance is not a substitute for
the catalog's required runtime source provenance: a real downloader must record
the upstream checksum and other required source metadata before publication.

`wikimedia_fixtures.py` validates those identities and can atomically publish a
canonical fixture manifest for a complete scenario. The `tiny` and `demo`
profiles select fixed hour subsets and enforce byte, record, and runtime limits
before publication; they never represent production-volume downloads.
Recent-change replay cases are already allowlisted projections and intentionally
contain no identity or free-text fields. Projection validates the scalar fields
against `recentchange_events` in the catalog and rejects raw or nested sensitive
event forms.

## Fixture publication filesystem boundary

Fixture publication accepts a caller-selected, trusted local destination. It
rejects existing symlinks along the currently inspected destination path and
uses same-filesystem hard-link creation for the final manifest, so normal
concurrent publishers cannot clobber an existing manifest. If hard links or
staging writes are unavailable, publication fails closed and writes no manifest.
This is not a claim of resistance to a malicious same-user process replacing an
ancestor after inspection; callers needing that threat model must provide a
native filesystem helper. The hard-link requirement is intentionally less
portable than a copy fallback, because a non-atomic fallback would violate the
no-clobber contract.
