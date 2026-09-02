# Product Specification

## Product overview

LakeOps Agent is a governed data operations copilot for a public Wikimedia
analytics pipeline. It connects operational diagnosis, natural-language
analytics, and controlled remediation in one demonstrable workflow.

The product is a portfolio project, not a production service. Its implementation
must nevertheless expose the controls, evidence, and trade-offs required to move
a proof of concept toward production.

## Target users

| User | Goal |
|---|---|
| Data platform engineer | Detect and diagnose pipeline, freshness, schema, and partition failures |
| Network operations analyst | Ask operational questions without manually writing SQL |
| Technical decision-maker | Understand the deployment, security, cost, and production-readiness trade-offs |

## Primary demo scenario

The platform receives two public Wikimedia sources:

- Hourly per-page traffic files from the Wikimedia pageviews dump.
- Live recent-change events from the Wikimedia EventStreams SSE endpoint.

A daily batch consumes all 24 UTC pageview files while the streaming path
materializes allowlisted editing-activity windows. During the primary demo, one
hourly pageview object is withheld, creating an incomplete daily traffic result.
The user asks why reported English Wikipedia traffic dropped. The agent generates
safe SQL, proves that the apparent drop is a missing source partition rather than
a real demand change, proposes an exact backfill, requests human approval, and
verifies the newly published complete manifest.

## Core capabilities

### Governed text-to-SQL

1. Resolve business terms and relevant datasets from the repository catalog.
2. Ask for clarification when the question is materially ambiguous.
3. Generate SQL against catalog-registered logical views only.
4. Validate the SQL AST, referenced objects, operation type, and resource limits.
5. Execute through DuckDB and return the answer, SQL, sources, and execution
   metadata.

### Data operations

1. Inspect pipeline runs, dataset freshness, schemas, and partition manifests.
2. Collect evidence and describe the likely impact.
3. Produce a deterministic remediation plan.
4. Require explicit approval for any write or control-plane action.
5. Execute an idempotent operation and verify its result.

### Batch pipeline

The batch path discovers and downloads 24 hourly pageview files for a UTC day,
records source provenance and checksums, validates and normalizes the four-field
source format, writes immutable Bronze evidence and Silver Parquet objects, then computes
the catalog-defined daily project-traffic KPI only after all 24 accepted hours
succeed. Governed queries can select only catalog-registered views and fields;
they cannot supply SQL or storage paths.

For the missing-hour demonstration, the committed bounded fixture produces
governed freshness evidence with 23 accepted objects out of 24 expected. It
does not produce a partial traffic metric. The complete-day fixture reports
24/24, so a real traffic change remains distinguishable from an ingestion gap.

### Streaming pipeline

The streaming path consumes Wikimedia recent-change SSE events and can relay the
allowlisted projection through Azure Event Hubs. A stream processor checkpoints
consumption, validates events, removes identity and free-text fields before
durable storage, and writes bounded micro-batches as immutable Parquet objects.
Downstream transforms join traffic and editing activity by project, page, and
event-time window.

## Data contract

The MVP uses real public Wikimedia sources at a scale suitable for data-pipeline
operations:

- Pageview dump files are hourly, roughly tens of compressed megabytes each; a
  complete daily profile processes 24 files and about 1-2 GB compressed.
- Recent-change events are consumed from a live SSE stream, with deterministic
  recorded projections reserved only for tests and replay.
- Raw recent-change identity and free-text fields are discarded before durable
  storage, logs, traces, fixtures, or governed queries.

The versioned dataset contract in `data/catalog/catalog.json` is the single
validated authority for dataset and logical view names, storage prefixes,
partition keys, column types, business descriptions and terms, approved joins,
KPI definitions with units and aggregation rules, freshness objectives, and
sensitivity labels. It is the only source from which queryable DuckDB views may
be created.

## Security and control boundaries

- Analytics queries are read-only.
- SQL is rejected unless it passes deterministic validation.
- Storage access uses least-privilege managed identities.
- Operational writes require explicit human approval.
- Every model call, tool call, query, approval, and action has a correlated trace.
- Sensitive telemetry is redacted before trace export.

## Evaluation targets

The project measures:

- Executed-answer accuracy rather than SQL string similarity.
- Ambiguity detection and correct refusal.
- Forbidden query and unauthorized dataset rejection.
- Data incident diagnosis and remediation-plan accuracy.
- Latency, token usage, bytes scanned, and estimated request cost.

## Delivery stages

1. Repository, architecture, Terraform root, and Wikimedia data contract.
2. Batch and streaming pipelines with local emulation and Azure deployment.
3. Safe text-to-SQL workflow and evaluation dataset.
4. Data operations diagnosis and one human-approved remediation action.
5. End-to-end observability, continuous evaluation, and public demo materials.
