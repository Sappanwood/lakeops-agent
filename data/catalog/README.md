# Dataset catalog

The versioned dataset contract in `catalog.json` is the single validated
authority for dataset, schema, business term, join, KPI, partition, freshness,
sensitivity, storage-prefix, and logical view metadata. Components must not
invent competing field or business definitions.

## Contents

`catalog.json` declares:

- Wikimedia hourly pageview and live recent-change sources.
- Seven logical datasets with schema, primary keys, partition rules, freshness
  objectives, and sensitivity labels.
- The approved join graph between datasets.
- Versioned KPI definitions with units, formulas, and SLA thresholds.
- Business terms with aliases and mappings to datasets and fields.
- The logical views that form the only permitted governed text-to-SQL surface.
- Tiny, demo, daily, and soak volume profiles from tens of megabytes through
  multi-gigabyte runs.
- The fixed missing-hour demo scenario and approval-bound backfill operation.
- The privacy projection that removes Wikimedia user identity and free-text fields
  before durable stream storage.

## Validation and consumption

- `catalog_version` bumps whenever the contract changes materially.
- `validator.py` checks the complete major-version-2 source, storage,
  publication, privacy, dataset, KPI, business-term, and query contracts. Join
  cardinality must be supported by dataset primary keys, and every logical view
  declares the datasets and joins from which its fields originate.
- Consumers call `validate_catalog()` and derive their source, dataset, and view
  metadata from its complete return value. ID-bearing collections are sorted,
  and `canonical_metadata_json()` provides a stable serialized representation
  for manifests, fixtures, governed queries, evaluations, and contract snapshots.
- Only `lakeops/catalog@1` with catalog major version 2 is supported. Older or
  unknown contracts fail explicitly and are not coerced.
- Physical storage roots are deployment configuration. This contract defines
  logical prefixes only.
