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

## Local publication boundary

The destination must be a trusted local directory. The publisher rejects static
symlinks in its inspected path and uses same-filesystem POSIX hard links with
exclusive directory creation, so normal concurrent runs cannot overwrite a
Bronze object or manifest. A repeated `--run-id` fails rather than replacing
evidence. This does not resist a malicious same-user process swapping an
ancestor after inspection; that stronger threat model requires a native helper.
Hard-link publication is intentionally required, so filesystems without POSIX
hard links fail closed instead of falling back to a non-atomic copy.
