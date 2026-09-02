# Tests

This directory is reserved for cross-component contract, integration, and public
demo acceptance tests. Component-local unit tests should remain with their owning
package.

Run the current Python contract and pipeline checks with:

```bash
uv run python -m unittest discover -s tests
```

Fixture tests compare canonical source records and manifests. They do not
compare generated Parquet bytes, which can vary across dependency versions.

`test_bronze_pageviews.py` uses a deterministic in-memory HTTP transport. It
does not download Wikimedia bulk data, while covering the runtime Bronze
publisher's source provenance, invalid-input diagnostics, containment, and
normal-concurrency no-clobber behavior.

`test_silver_pageviews.py` covers accepted Bronze-to-Silver normalization,
canonical Bronze manifest identity, source provenance, typed Parquet schema and
deterministic reads, duplicate primary keys, source-to-manifest checksum joins,
partition boundaries, bounded-memory normalization, and local no-clobber
publication.
