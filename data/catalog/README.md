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
- The contract validator (backlog item LKO-013) checks schema conformance,
  enum vocabularies, join references, and version behavior. Consumers (batch
  pipeline, governed query tool, evaluation) derive catalog metadata only from
  validated copies of this file.
- Physical storage roots are deployment configuration. This contract defines
  logical prefixes only.
