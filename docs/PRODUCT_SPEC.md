# Product Specification

## Product overview

LakeOps Agent is a governed data operations copilot for a fictional
telecommunications analytics platform. It connects operational diagnosis,
natural-language analytics, and controlled remediation in one demonstrable
workflow.

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

The fictional platform receives telecommunications network telemetry and daily
reference data:

- Streaming cell-site and service-quality telemetry.
- Batch network inventory, maintenance windows, and incident records.
- Curated service-level indicators derived from both paths.

During the demo, a delayed or malformed partition causes an SLA metric to become
stale. The user asks the agent why a regional metric changed. The agent generates
safe SQL, identifies the data quality issue, proposes remediation, requests human
approval, and verifies the repaired state.

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

The batch path generates or ingests reference data, validates its schema and
quality, writes immutable Parquet objects, and publishes a dataset manifest only
after successful validation.

### Streaming pipeline

The streaming path publishes synthetic telemetry to Azure Event Hubs. A stream
processor checkpoints consumption, validates events, and writes bounded
micro-batches as immutable Parquet objects. Downstream batch transforms merge the
stream and reference domains into curated analytical datasets.

## Data contract

The initial public dataset uses synthetic data only. The planned dataset registry
contains:

- Dataset and logical view names.
- Storage prefixes and partition keys.
- Column types and business descriptions.
- Approved join relationships.
- KPI definitions, units, and aggregation rules.
- Freshness objectives and data sensitivity labels.

The registry is version-controlled and is the only source from which queryable
DuckDB views may be created.

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

1. Repository, architecture, Terraform root, and synthetic data contract.
2. Batch and streaming pipelines with local emulation and Azure deployment.
3. Safe text-to-SQL workflow and evaluation dataset.
4. Data operations diagnosis and one human-approved remediation action.
5. End-to-end observability, continuous evaluation, and public demo materials.
