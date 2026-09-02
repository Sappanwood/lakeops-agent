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
source-to-manifest join, and catalog-defined `pageviews_hourly` schema. It
decodes each Pageviews title once, normalizes spaces to underscores, derives the
UTC logical partition from Bronze, validates positive view counts, and detects
duplicate primary keys through a spill-capable DuckDB external aggregation
before any Silver output becomes visible.

```bash
python3 -m pipelines.batch.silver \
  --bronze-manifest data/generated/wikimedia-tiny/manifests/pageviews_hourly/partition_date=2026-08-01/<bronze-run-id>.json \
  --destination data/generated/wikimedia-tiny \
  --run-id silver-20260801-01
```

The local writer streams normalized records into temporary disk-backed staging,
uses DuckDB with a configured 256 MB buffer-memory limit and staging-contained
spill directories for external primary-key aggregation,
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
than replacing prior evidence.

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
