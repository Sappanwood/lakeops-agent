# Terraform infrastructure

This Terraform root targets a disposable Azure portfolio environment. Stable
resources use AzureRM. AzAPI is reserved for required Azure control-plane
capabilities that AzureRM does not expose.

## Initial usage

```bash
terraform init
terraform fmt -check -recursive
terraform validate
terraform plan -var subscription_id=<azure-subscription-id>
```

The initial configuration creates only the resource group. Later changes will add
services incrementally and must update `docs/ARCHITECTURE.md` and
`docs/COST_MODEL.md`.

Do not commit Terraform state or plan files. A remote Azure Storage backend will
be introduced before shared deployment workflows are enabled.
