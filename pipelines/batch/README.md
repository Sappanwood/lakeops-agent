# Batch pipeline

This directory will contain the daily Wikimedia pageview job: discover and
download 24 hourly dump files, record checksums and source metadata, validate and
normalize the public four-field format, publish immutable Parquet, and expose a
daily manifest only when all expected hours are complete.
