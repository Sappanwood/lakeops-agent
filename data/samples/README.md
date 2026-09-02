# Sample data

This directory is reserved for small deterministic Wikimedia fixtures that are
safe for a public repository. Fixtures must be minimal projections of the public
source contracts, include provenance, and exclude recent-change identity and
free-text fields.

Upstream pageview dumps, live stream captures, generated Parquet, and volume-test
data belong under `data/generated/` and are ignored by Git. Bulk source data must
be reproducible from a manifest and is never committed.
