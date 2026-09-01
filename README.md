# LakeOps Agent

LakeOps Agent is a portfolio-grade Azure data operations copilot for a fictional
telecommunications data platform. It combines governed text-to-SQL, batch and
streaming data pipelines, and human-approved operational actions.

The project is designed to demonstrate how an agent prototype can move toward a
production deployment through explicit security boundaries, evaluation,
observability, and infrastructure as code.

## Planned capabilities

- Ask business and operational questions in natural language.
- Generate and execute read-only SQL against curated Parquet datasets.
- Diagnose data freshness, schema drift, missing partitions, and failed jobs.
- Propose operational remediation and require approval before mutation.
- Simulate both scheduled batch ingestion and event-driven telemetry ingestion.
- Trace and evaluate agent, tool, and query execution end to end.

## Architecture

The frontend is hosted by Azure Static Web Apps. FastAPI and LangGraph run
together as a self-hosted agent service on Azure Container Apps. Pipeline jobs
and optional streaming workers also use Container Apps. Microsoft Foundry
provides model access and evaluation integration rather than hosting the agent
runtime. Data is stored as Parquet in Azure Data Lake Storage Gen2 and queried
with DuckDB.

See [Architecture](docs/ARCHITECTURE.md), [Product specification](docs/PRODUCT_SPEC.md),
and [Cost model](docs/COST_MODEL.md) for the current design.

## Repository status

The repository currently contains the initial architecture, product contract,
cost assumptions, component boundaries, and Terraform root configuration.
Application and pipeline implementation will be added incrementally.

## Repository layout

```text
apps/                React UI and FastAPI agent service
agent/               LangGraph agent and governed tools
pipelines/           Batch producer, stream producer, and stream processor
data/                Dataset catalog and small public sample data
evaluations/         Agent and text-to-SQL evaluation datasets
infra/terraform/     Azure infrastructure
docs/                Public product, architecture, and cost documentation
tests/               Cross-component and acceptance tests
```

## Infrastructure

Terraform targets Azure Japan East by default. The initial configuration creates
only a resource group; service modules will be added with the implementation so
that infrastructure remains reviewable and deployable in small increments.

```bash
cd infra/terraform
terraform init
terraform plan -var subscription_id=<azure-subscription-id>
```

No Azure resources are created until `terraform apply` is run explicitly.
