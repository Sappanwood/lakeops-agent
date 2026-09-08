# Terraform infrastructure

This Terraform root targets a disposable Azure portfolio environment. Stable
resources use AzureRM. AzAPI is reserved for required Azure control-plane
capabilities that AzureRM does not expose.

## Validation

Use Terraform 1.11 or newer (validated locally with 1.12.1). The minimum version
covers plan-time mock values through `override_during = plan`.

```bash
terraform init -backend=false -lockfile=readonly
terraform fmt -check -recursive
terraform validate
terraform test
```

Every test uses mocked providers and `command = plan`; tests require no Azure
credentials and create no cloud resources. They check the opt-in boundary,
model/version/SKU, Entra authentication, account-scoped inference RBAC, connection
outputs, and invalid capacity/name inputs. Provider plugins must be allowed to
open their local IPC socket even for offline validation.

Do not commit Terraform state or plan files. A remote Azure Storage backend will
be introduced before shared deployment workflows are enabled.

## Foundry model preparation

The default `foundry = null` creates only the resource group. Setting the object
adds exactly four resources:

- An `AIServices` Foundry account with a custom subdomain and system identity.
- The `lakeops-chat` deployment of `gpt-5.6-luna`, version `2026-07-09`, using
  `GlobalStandard` and `NoAutoUpgrade`.
- A user-assigned managed identity for the future agent workload.
- `Cognitive Services OpenAI User` for that identity, scoped to this account.

The account uses a public network endpoint with mandatory Entra authentication;
local/API-key authentication is disabled. No key is output or used. The account
identity and inference caller identity are distinct. No caller is given model
deployment, role management, or subscription-level permissions by this config.
Foundry projects and hosted agent infrastructure are not required for the current
account-level model API. The application runtime remains self-hosted.

Japan East is the default resource region. Global Standard inference can run
outside Japan; it is not a Japan-only data-processing boundary. The reviewed model
and version are fixed in `foundry.tf`; changing them requires current model,
regional availability, quota, and price verification.

```bash
cp foundry.tfvars.example foundry.tfvars
# Set the target subscription and a globally unique account name in the file.
terraform plan -var-file=foundry.tfvars -out=foundry.tfplan
terraform show foundry.tfplan
```

`foundry.tfvars` and the plan are ignored by Git. Do not use the example UUID or
account name unchanged. Capacity defaults to 10 and accepts integer values 1-10,
representing 1,000-10,000 TPM. It reserves model quota, not provisioned hourly
capacity, and is not a monetary spending cap. Dynamic throttling is disabled.
Review both requested capacity and subscription quota before applying.

## Before a live plan or deployment

1. Confirm the intended subscription explicitly. `az account show` describes the
   CLI default; it does not establish this project's deployment target.
2. Check registration of `Microsoft.CognitiveServices` and
   `Microsoft.ManagedIdentity` in that subscription. AzureRM automatic provider
   registration is disabled, so a plan cannot silently register subscription-wide
   providers. Registration, if needed, is an explicit prerequisite.
3. Inspect subscription-visible models and quota in the selected region:

   ```bash
   az cognitiveservices model list --subscription <subscription-id> --location japaneast
   az cognitiveservices usage list --subscription <subscription-id> --location japaneast
   ```

   Confirm `gpt-5.6-luna` / `2026-07-09`, `GlobalStandard`, available capacity and
   account-name availability. Public documentation and mocked plans cannot prove
   subscription eligibility or current service capacity.
4. The deployment principal needs resource creation and scoped role-assignment
   permissions. Those control-plane permissions are separate from inference RBAC.
5. Review the actual saved plan, expected resource count, current prices, and an
   agreed cost cap before explicitly approving `terraform apply foundry.tfplan`.
   Applying a plan is not part of the validation commands above.

## Connection and teardown

After deployment, `terraform output -json foundry` supplies the OpenAI v1 base
URL, `lakeops-chat` deployment name, model identity, and managed identity resource
and client IDs. Attach that user-assigned identity to the future Container App;
it cannot be impersonated from a developer laptop merely by setting its client
ID. A local developer needs a separately authorized Entra inference role. No
local developer role is automatically assigned by this configuration.

The later model client should use the OpenAI v1 Responses API, the deployment
name as `model`, and an Entra token provider. Model SDK/client implementation and
live inference smoke tests are separate from this infrastructure preparation.

Keep the same tfvars and state for lifecycle operations. Removing `foundry` or
setting it to null after deployment plans deletion of its resources. Review a
destroy plan before any teardown; do not discard state to reset an environment.
The provider does not automatically purge soft-deleted Foundry accounts. Name
reuse may require an explicit recovery or purge decision; no purge is performed
as a side effect of routine teardown. `NoAutoUpgrade` also means a retired model
version needs an explicit reviewed update.

## Official references (checked 2026-09-08)

- [Foundry Terraform setup](https://learn.microsoft.com/en-us/azure/foundry/how-to/create-resource-terraform)
- [Model availability](https://learn.microsoft.com/en-us/azure/foundry/foundry-models/concepts/models-sold-directly-by-azure)
- [Region availability](https://learn.microsoft.com/en-us/azure/foundry/foundry-models/concepts/models-sold-directly-by-azure-region-availability)
- [OpenAI User RBAC](https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles/ai-machine-learning#cognitive-services-openai-user)
- [Responses API](https://learn.microsoft.com/en-us/azure/foundry/openai/how-to/responses)
- [Provider account contract](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/cognitive_account)
- [Provider deployment contract](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/cognitive_deployment)
