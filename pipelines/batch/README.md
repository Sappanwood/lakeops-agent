# Batch pipeline

`bronze.py` downloads and validates raw Wikimedia Pageviews objects, then
publishes immutable Bronze evidence and an accepted manifest. It does not perform
Silver normalization or write Parquet; those are separate stages.

The source date is the UTC logical day. Wikimedia names files with the end of the
captured hour, so a complete logical day `D` uses `D 01:00` through `D 23:00` and
`D+1 00:00`. The exact URL list is calculated from the pinned `--partition-date`
and profile, rather than being discovered from a directory listing.

Run against a date whose source files are already available:

```bash
python3 -m pipelines.batch.bronze \
  --partition-date 2026-08-01 \
  --profile tiny \
  --destination data/generated/wikimedia-tiny

python3 -m pipelines.batch.bronze \
  --partition-date 2026-08-01 \
  --profile demo \
  --destination data/generated/wikimedia-demo

python3 -m pipelines.batch.bronze \
  --partition-date 2026-08-01 \
  --profile daily \
  --destination data/generated/wikimedia-daily
```

`tiny`, `demo`, and `daily` select exactly 1, 6, and 24 hourly source objects.
The latter can download roughly 1–2 GB compressed; `data/generated/` is ignored
by Git, and no bulk Pageviews data is committed. The job requires HTTP 200 plus
`Content-Length`, `ETag`, and `Last-Modified`; it computes SHA-256 while
streaming the compressed object and rejects malformed, truncated, or
length-conflicting inputs before publication.

Each accepted manifest records source URL, response metadata, SHA-256, retrieval
time, capture-end label, logical UTC hour, record count, object path, and run
metadata. It becomes visible only after all source objects are durable.

## Silver normalization

`silver.py` consumes only an accepted Bronze Pageviews manifest at its canonical
destination-relative identity
`manifests/pageviews_hourly/partition_date=<date>/<bronze-run-id>.json`. It
revalidates the profile's exact continuous hour set, canonical Wikimedia source
URL, runtime provenance, object path, content length, SHA-256, gzip row schema,
source-to-manifest join, and catalog-defined `pageviews_hourly` schema. Wikimedia
already publishes `page_title` as a URL-decoded canonical DBkey, so Silver
preserves literal percent characters and only normalizes spaces to underscores
defensively. It derives the UTC logical partition from Bronze, validates
positive view counts, and detects duplicate primary keys one hour at a time
through spill-capable DuckDB external aggregation before any Silver output
becomes visible. Cross-file collisions are impossible after the manifest's
unique continuous logical-hour set is validated because `window_end` is part of
the primary key.

```bash
python3 -m pipelines.batch.silver \
  --bronze-manifest data/generated/wikimedia-tiny/manifests/pageviews_hourly/partition_date=2026-08-01/<bronze-run-id>.json \
  --destination data/generated/wikimedia-tiny \
  --run-id silver-20260801-01
```

The local writer streams normalized records into temporary disk-backed staging,
uses DuckDB with a configured 256 MB buffer-memory limit and staging-contained
spill directories for per-hour external primary-key aggregation,
and publishes one immutable typed Parquet object per logical hour below
`silver/pageviews_hourly/`. It validates each physical Parquet schema and row
count before publication, then publishes an accepted Silver manifest below
`manifests/pageviews_hourly/`. A rejected input produces only immutable
destination-relative rejection evidence below `quarantine/pageviews_hourly/`;
it never publishes partial Silver data or mutates Bronze bytes.

## Local publication boundary

The destination must be a trusted local directory. The publisher rejects static
symlinks in its inspected path and uses same-filesystem POSIX hard links with
exclusive directory creation, so normal concurrent runs cannot overwrite a
Bronze object or manifest. A repeated `--run-id` fails rather than replacing
evidence. This does not resist a malicious same-user process swapping an
ancestor after inspection; that stronger threat model requires a native helper.
Hard-link publication is intentionally required, so filesystems without POSIX
hard links fail closed instead of falling back to a non-atomic copy.

Silver has the same trusted-directory, static-symlink-containment, and normal
concurrent no-clobber boundary. It does not claim resistance to a malicious
same-user ancestor swap; that stronger boundary needs a native helper.

## Gold metrics and governed views

`gold.py` accepts only a canonical accepted **daily** Silver manifest. It
rechecks every declared Silver object path, checksum, Parquet schema, row count,
and continuous 24-hour input set before it aggregates the catalog's
`kpi.project_daily_views` into `project_traffic_daily`. The published Gold
Parquet object and manifest are immutable; a repeated run identity fails rather
than replacing prior evidence. Exact daily unique-page counts use an external
sort over `(project_code, page_title)` with a 256 MB DuckDB buffer limit rather
than an unbounded distinct hash table. `input_hour_count` and `is_complete`
describe the accepted 24-hour input partition; a project does not need traffic
in every hour to be complete.

```bash
python3 -m pipelines.batch.gold \
  --silver-manifest data/generated/wikimedia-daily/manifests/pageviews_hourly/partition_date=2026-08-01/<silver-run-id>.json \
  --destination data/generated/wikimedia-daily \
  --run-id gold-20260801-01
```

`open_governed_query()` exposes a narrow Python query API: callers supply an
exact catalog view name plus an exact subset of that view's catalog fields. It
does not accept SQL, file paths, relation names, or arbitrary columns. Only the
catalog views with accepted bound inputs are registered. Until the streaming pipeline
publishes accepted Silver evidence, views which require editing-activity inputs
fail closed with `view_unavailable`; no empty relation, null join, or fabricated
operational record is exposed.

`materialize_fixture_ingestion_freshness()` is the bounded local demonstration
of operational evidence. It derives `v_ingestion_freshness` from the committed
`complete_day` or `missing_hour_day` fixture manifest: 24/24 is `complete`,
while 23/24 is `missing`. A missing-day evidence manifest does not create
`project_traffic_daily`; queries of that view remain unavailable, keeping a
pipeline gap distinct from a real traffic change on a complete day.

## End-to-end local verification

`local.py` coordinates the daily Bronze, Silver, and Gold stages and publishes
`lakeops/batch-pipeline-manifest@1` only after it has revalidated the complete
manifest lineage, all 49 immutable objects, checksums, row counts, canonical
paths, and the governed Gold query surface. It applies a durability barrier to
all referenced objects and stage manifests before the final manifest becomes
visible.

This offline command runs the committed complete-day fixture through the real
Bronze, Silver, and Gold implementations without Azure credentials or network
access:

```bash
uv run python -m pipelines.batch.local \
  --partition-date 2024-01-01 \
  --source fixture \
  --destination data/generated/wikimedia-local \
  --run-id local-20240101
```

Use the same coordinator with `--source live` and an available Wikimedia date
for the real daily input, which is commonly around 1–2 GB compressed:

```bash
uv run python -m pipelines.batch.local \
  --partition-date 2026-08-01 \
  --source live \
  --destination data/generated/wikimedia-live-20260801 \
  --run-id live-20260801 \
  --download-workers 2
```

`--download-workers` is bounded from 1 through 8. The standalone Bronze command
remains serial by default; the end-to-end coordinator defaults to two workers
so the daily profile does not serialize 24 independent HTTP transfers or create
an unnecessarily aggressive burst. Live HTTP 429 and selected 5xx responses are
retried up to four attempts with bounded exponential or `Retry-After` delays.

The final manifest is written below
`manifests/batch_pipeline/partition_date=<date>/`. A failed stage cannot publish
that authoritative manifest or replace an earlier accepted run. If the process
is interrupted before final publication, rerunning the same command and run ID
reuses only canonical stage manifests and objects that pass full validation;
corrupt or missing evidence fails closed. Repeating an already accepted run ID
returns `publication_conflict`.
