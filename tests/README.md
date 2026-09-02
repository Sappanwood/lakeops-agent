# Tests

This directory is reserved for cross-component contract, integration, and public
demo acceptance tests. Component-local unit tests should remain with their owning
package.

Run the current dataset-contract and fixture checks with:

```bash
python3 -m unittest discover -s tests
```

Fixture tests compare canonical source records and manifests. They do not
compare generated Parquet bytes, which can vary across dependency versions.

`test_bronze_pageviews.py` uses a deterministic in-memory HTTP transport. It
does not download Wikimedia bulk data, while covering the runtime Bronze
publisher's source provenance, invalid-input diagnostics, containment, and
normal-concurrency no-clobber behavior.
