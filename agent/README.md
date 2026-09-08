# Agent

`query_safety.py` implements the deterministic generated-SQL boundary. The
LangGraph workflow, typed tools, state model, and operation approval transitions
remain planned. The graph will be independent of HTTP transport and loaded by
FastAPI under `apps/api/`.

## SQL boundary

A service constructs one `SqlExecutor(registered_views, limits=QueryLimits())`.
`registered_views` is a set or frozenset of exact logical view names loaded from
the validated catalog. Every `execute(sql, bindings)` invocation validates the
SQL again; `ValidatedSql` is diagnostic output, never an execution capability.
The executor only runs SQL regenerated from the accepted DuckDB-dialect AST.

The supported subset is one SELECT with projections, ordinary arithmetic and
comparisons, filters, grouping, ordering, limits, joins, nested SELECTs, and
COUNT/SUM/MIN/MAX/AVG/ABS/ROUND/COALESCE/NULLIF/CASE. Exact AST node types form a
positive allowlist. CTEs, UNION, window functions, casts, arbitrary functions,
table functions, schema-qualified objects, parameters, mutation, and additional
statements fail closed. Every base relation must be an exact registered name;
DuckDB then binds columns against the actual host-created views. Model claims
about validation or required configuration have no authority.

`bindings` maps registered names to immutable `BoundView(query, scan_bytes)`
values. Both fields are **trusted host inputs**, never tool arguments exposed to
the model: `query` is the host-generated SELECT defining the view and
`scan_bytes` is the full byte size of its accepted bound input objects. The
executor creates only referenced views in a fresh in-memory DuckDB instance.
The manifest-binding and source-evidence adapter is a separate integration layer;
this low-level API does not itself validate manifests, storage paths, catalog
files, or view definitions. The existing batch view/field API remains separate.

`SqlResult` returns canonical `sql`, referenced `views` (including repeated
references), `columns`, `rows`, `scan_bytes_upper_bound`, and `elapsed_seconds`.
Policy, missing binding, execution, and worker failures raise `QuerySafetyError`
with stable codes and sanitized messages. No partial rows are returned on failure.

## Resource policy

| Control | Default | Enforcement |
|---|---|---|
| Wall time | 10 seconds | Parent deadline covers validation, spawn, binding, execution, fetch and IPC; overdue worker is killed and reaped |
| DuckDB memory | 128 MiB | Engine buffer budget, one thread, disk spill disabled |
| Worker memory | 1 GiB | POSIX `RLIMIT_AS` hard address-space cap before opening the query connection |
| Input admission | 256 MiB | Full bound input bytes per syntactic base-view reference, before worker creation |
| Rows | 1,000 | Fetch at most limit + 1 and reject excess |
| Result bytes | 4 MiB | Serialized columns/rows payload is bounded before IPC |
| Concurrent queries | 2 | Shared executor semaphore; excess requests fail immediately |

The input admission value is a conservative bound on the inputs admitted by the
query, **not measured physical I/O or optimizer-estimated scan bytes**. It counts
self-joins repeatedly, takes no pruning credit for WHERE or LIMIT, and rejects a
missing binding even when LIMIT is zero. It does not count repeated physical
reads of an input during execution. The process deadline and memory limits bound
that execution independently.

DuckDB's buffer budget does not cover all Python or engine allocations; the
separate worker address-space cap covers those allocations after worker startup.
Startup imports occur before that cap is installed. Results are fully produced
inside the worker; only the bounded serialized payload reaches the parent.
A worker that cannot install OS limits fails closed. Execution currently requires
POSIX with `RLIMIT_AS` (tested on Linux); no native helper is required. The parent
is trusted and not itself memory-capped by this component. Spawn callers in script
entrypoints must use the usual `if __name__ == "__main__":` guard.

The semaphore belongs to a service-owned executor instance and includes validation
and worker cleanup. Do not create an executor per request. Multiple service
processes need a deployment-level concurrency allocation; this is not a
distributed quota. Workers disable extension auto-install/loading, Python
replacement scans, core dumps, and configuration changes after view registration.
AST validation is the model-to-storage boundary; trusted binding SQL may access
host-selected local Parquet. This is not an OS sandbox against malicious host
Python code or compromised native libraries.

## Verification and upstream contracts

Run `uv run python -m unittest discover -s tests` from the repository root.
`tests/test_query_safety.py` covers allowed analytics, adversarial SQL, admission
limits, actual memory exhaustion, expensive-query timeout, overlapping requests,
worker cleanup, sanitized errors, and oversized single-row results. On Linux it
also inspects the running worker's hard address-space limit.

The AST integration follows [SQLGlot's parser and expression API](https://sqlglot.com/sqlglot.html).
Runtime settings follow [DuckDB configuration](https://duckdb.org/docs/current/configuration/overview)
and [Python client fetch APIs](https://duckdb.org/docs/current/clients/python/reference/).
The worker cap uses [Python's POSIX resource API](https://docs.python.org/3/library/resource.html).
