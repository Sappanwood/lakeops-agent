resource "azurerm_cognitive_account" "foundry" {
  count = var.foundry == null ? 0 : 1

  name                          = var.foundry.account_name
  location                      = azurerm_resource_group.main.location
  resource_group_name           = azurerm_resource_group.main.name
  kind                          = "AIServices"
  sku_name                      = "S0"
  custom_subdomain_name         = var.foundry.account_name
  project_management_enabled    = true
  local_auth_enabled            = false
  public_network_access_enabled = true
  tags                          = local.common_tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_cognitive_deployment" "chat" {
  count = var.foundry == null ? 0 : 1

  name                       = "lakeops-chat"
  cognitive_account_id       = azurerm_cognitive_account.foundry[0].id
  version_upgrade_option     = "NoAutoUpgrade"
  dynamic_throttling_enabled = false

  model {
    format  = "OpenAI"
    name    = "gpt-5.6-luna"
    version = "2026-07-09"
  }

  sku {
    name     = "GlobalStandard"
    capacity = var.foundry.capacity
  }
}

resource "azurerm_user_assigned_identity" "agent" {
  count = var.foundry == null ? 0 : 1

  name                = "id-${local.base_name}-agent"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tags                = local.common_tags
}

resource "azurerm_role_assignment" "model_inference" {
  count = var.foundry == null ? 0 : 1

  scope                            = azurerm_cognitive_account.foundry[0].id
  role_definition_id               = "/subscriptions/${var.subscription_id}/providers/Microsoft.Authorization/roleDefinitions/5e0bd9bd-7b93-4f28-af87-19fc36ad61bd"
  principal_id                     = azurerm_user_assigned_identity.agent[0].principal_id
  principal_type                   = "ServicePrincipal"
  skip_service_principal_aad_check = true
}
