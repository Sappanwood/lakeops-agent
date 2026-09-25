# LakeOps Agent Repository Instructions

Read [README.md](README.md) for the public project overview.

## Non-negotiable constraints

- Write documentation, code comments, and docstrings in English.
- Treat the project as a pre-1.0 demo with no backward-compatibility guarantee.
  Breaking changes to project-owned APIs, schemas, fixtures, and disposable demo
  infrastructure are always allowed. Update code, tests, and documentation
  together instead of adding compatibility layers, dual writes, deprecation
  windows, or data migrations unless a current acceptance criterion explicitly
  requires them. This does not authorize destructive changes to external data,
  shared workspace resources, or infrastructure outside the resolved project
  scope.
- Keep generated SQL read-only and restricted to catalog-registered views.
- Never allow an LLM response to bypass deterministic SQL validation, resource
  limits, identity checks, or operation approval.
- Authenticate Azure workloads with Microsoft Entra managed identity. Do not add
  storage account keys, connection strings, or committed secrets.
- Keep batch and stream outputs immutable. Publish new Parquet objects instead of
  mutating an object in place.
- Do not introduce Iceberg, Delta Lake, or another table format until a concrete
  requirement needs transactions, snapshots, multi-writer commits, or schema
  evolution beyond the repository catalog contract.

## External documentation

Before changing Azure resources, SDK contracts, model deployments, service
limits, or pricing assumptions, verify the current official documentation:

- Microsoft Foundry: <https://learn.microsoft.com/azure/foundry/>
- Azure Container Apps: <https://learn.microsoft.com/azure/container-apps/>
- Azure Event Hubs: <https://learn.microsoft.com/azure/event-hubs/>
- Azure Storage: <https://learn.microsoft.com/azure/storage/>
- AzureRM provider: <https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs>
- DuckDB: <https://duckdb.org/docs/stable/>

## Documentation routing

| Document | Read when | Update when |
|---|---|---|
| [README.md](README.md) | On first entry or when preparing a public demo | Public scope, setup, or repository layout changes |
| [docs/PRODUCT_SPEC.md](docs/PRODUCT_SPEC.md) | Product behavior, users, workflows, or acceptance boundaries are relevant | User flows, data contracts, or roadmap scope changes |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Components, deployment, data flow, security, or technology choices are relevant | A component, service boundary, core data flow, or architectural constraint changes |
| [docs/COST_MODEL.md](docs/COST_MODEL.md) | Estimating or changing deployed Azure resources | Pricing date, resource sizing, operating mode, or service selection changes |
| [infra/terraform/README.md](infra/terraform/README.md) | Provisioning or changing Azure infrastructure | Provider, state, deployment, or teardown behavior changes |

## Project operations and dormant status

This project is dormant as of 2026-09-25 and is not registered in the active ProjectOps manifest.
Keep its code and historical artifacts in place. Read-only inspection does not authorize restarting
services or resuming the old backlog.

When the user selects a concrete restart scope, follow the Workspace AGENTS.md to register the
project in ProjectOps, update these instructions, and create only the newly agreed work. Do not
import or continue writing the old Workspace Control backlog. Explicit historical queries may
use the archived Catalog resolver described in the Workspace instructions.

Architecture decisions remain process records. Current public facts must stay self-contained
in this repository's documentation.

## Development conventions

- Python: 3.12 or newer, managed with `uv`.
- TypeScript: strict mode, managed with `pnpm`.
- Infrastructure: Terraform with AzureRM for stable resources and AzAPI only for
  Foundry capabilities not exposed by AzureRM.
- Use OpenTelemetry-compatible traces and redact prompts, tool arguments, and
  results before exporting sensitive content.
- Prefer the smallest implementation that satisfies the current acceptance
  criteria. Do not add speculative abstractions.

## Quality gates

Run the gates relevant to the changed component. The initial repository supports:

```bash
uv run python -m unittest discover -s tests
terraform fmt -check -recursive infra/terraform
terraform -chdir=infra/terraform init -backend=false -lockfile=readonly
terraform -chdir=infra/terraform validate
terraform -chdir=infra/terraform test
```

Terraform tests use mocked providers and plan-only runs; they require no Azure
credentials and must never provision cloud resources. Live plan/apply workflows
follow `infra/terraform/README.md` and require a confirmed target subscription.

As application packages are introduced, their lint, type-check, unit, integration,
and evaluation commands must be added here in the same change.

## Definition of done

- Tests cover expected behavior, failure behavior, and security-critical edges.
- Terraform changes are formatted and validated against pinned provider ranges.
- Public documentation reflects changes to architecture, user flows, data
  contracts, security boundaries, evaluation, or operations.
- No secret, generated dataset, Terraform state, or local environment artifact is
  committed.
