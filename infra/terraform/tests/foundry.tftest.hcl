mock_provider "azapi" {}

mock_provider "azurerm" {
  override_during = plan

  mock_resource "azurerm_cognitive_account" {
    defaults = {
      id       = "/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/rg-lakeops-dev/providers/Microsoft.CognitiveServices/accounts/lakeops-test"
      endpoint = "https://lakeops-test.cognitiveservices.azure.com/"
    }
  }

  mock_resource "azurerm_user_assigned_identity" {
    defaults = {
      principal_id = "00000000-0000-0000-0000-000000000002"
      client_id    = "00000000-0000-0000-0000-000000000003"
    }
  }
}

variables {
  subscription_id = "00000000-0000-0000-0000-000000000001"
}

run "disabled_by_default" {
  command = plan

  assert {
    condition = (
      length(azurerm_cognitive_account.foundry) == 0 &&
      length(azurerm_cognitive_deployment.chat) == 0 &&
      length(azurerm_user_assigned_identity.agent) == 0 &&
      length(azurerm_role_assignment.model_inference) == 0 &&
      output.foundry == null
    )
    error_message = "The default environment must not create model resources or expose connection settings."
  }
}

run "entra_only_luna_deployment" {
  command = plan

  variables {
    foundry = { account_name = "lakeops-test" }
  }

  assert {
    condition = (
      azurerm_cognitive_account.foundry[0].kind == "AIServices" &&
      azurerm_cognitive_account.foundry[0].sku_name == "S0" &&
      azurerm_cognitive_account.foundry[0].local_auth_enabled == false &&
      azurerm_cognitive_account.foundry[0].custom_subdomain_name == "lakeops-test" &&
      azurerm_cognitive_account.foundry[0].location == "japaneast" &&
      azurerm_cognitive_account.foundry[0].identity[0].type == "SystemAssigned"
    )
    error_message = "Foundry must use Entra authentication and the configured account identity and region."
  }

  assert {
    condition = (
      azurerm_cognitive_deployment.chat[0].cognitive_account_id == azurerm_cognitive_account.foundry[0].id &&
      azurerm_cognitive_deployment.chat[0].model[0].format == "OpenAI" &&
      azurerm_cognitive_deployment.chat[0].model[0].name == "gpt-5.6-luna" &&
      azurerm_cognitive_deployment.chat[0].model[0].version == "2026-07-09" &&
      azurerm_cognitive_deployment.chat[0].sku[0].name == "GlobalStandard" &&
      azurerm_cognitive_deployment.chat[0].sku[0].capacity == 10 &&
      azurerm_cognitive_deployment.chat[0].version_upgrade_option == "NoAutoUpgrade" &&
      azurerm_cognitive_deployment.chat[0].dynamic_throttling_enabled == false
    )
    error_message = "Use the reviewed Luna version with bounded standard capacity and no automatic upgrade."
  }

  assert {
    condition = (
      azurerm_role_assignment.model_inference[0].scope == azurerm_cognitive_account.foundry[0].id &&
      azurerm_role_assignment.model_inference[0].principal_id == azurerm_user_assigned_identity.agent[0].principal_id &&
      endswith(azurerm_role_assignment.model_inference[0].role_definition_id, "/5e0bd9bd-7b93-4f28-af87-19fc36ad61bd") &&
      azurerm_role_assignment.model_inference[0].principal_type == "ServicePrincipal"
    )
    error_message = "The agent identity must receive only the account-scoped OpenAI User role."
  }

  assert {
    condition = (
      output.foundry.openai_base_url == "https://lakeops-test.openai.azure.com/openai/v1/" &&
      output.foundry.deployment_name == azurerm_cognitive_deployment.chat[0].name &&
      output.foundry.managed_identity_client_id == azurerm_user_assigned_identity.agent[0].client_id &&
      toset(keys(output.foundry)) == toset([
        "account_id", "account_endpoint", "openai_base_url", "deployment_name",
        "model_name", "model_version", "managed_identity_id", "managed_identity_client_id"
      ])
    )
    error_message = "Connection settings must identify the actual deployment and caller identity."
  }
}

run "smallest_capacity" {
  command = plan
  variables {
    foundry = { account_name = "lakeops-test", capacity = 1 }
  }
  assert {
    condition     = azurerm_cognitive_deployment.chat[0].sku[0].capacity == 1
    error_message = "The smallest configured capacity must be preserved."
  }
}

run "reject_zero_capacity" {
  command = plan
  variables {
    foundry = { account_name = "lakeops-test", capacity = 0 }
  }
  expect_failures = [var.foundry]
}

run "reject_fractional_capacity" {
  command = plan
  variables {
    foundry = { account_name = "lakeops-test", capacity = 1.5 }
  }
  expect_failures = [var.foundry]
}

run "reject_excess_capacity" {
  command = plan
  variables {
    foundry = { account_name = "lakeops-test", capacity = 11 }
  }
  expect_failures = [var.foundry]
}

run "reject_invalid_account_name" {
  command = plan
  variables {
    foundry = { account_name = "not/a/subdomain" }
  }
  expect_failures = [var.foundry]
}
