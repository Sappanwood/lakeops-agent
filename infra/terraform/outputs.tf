output "resource_group_id" {
  description = "ID of the LakeOps Agent resource group."
  value       = azurerm_resource_group.main.id
}

output "resource_group_name" {
  description = "Name of the LakeOps Agent resource group."
  value       = azurerm_resource_group.main.name
}

output "foundry" {
  description = "Non-secret model connection settings and the managed identity to attach to the agent workload."
  value = var.foundry == null ? null : {
    account_id                 = azurerm_cognitive_account.foundry[0].id
    account_endpoint           = azurerm_cognitive_account.foundry[0].endpoint
    openai_base_url            = "https://${azurerm_cognitive_account.foundry[0].custom_subdomain_name}.openai.azure.com/openai/v1/"
    deployment_name            = azurerm_cognitive_deployment.chat[0].name
    model_name                 = azurerm_cognitive_deployment.chat[0].model[0].name
    model_version              = azurerm_cognitive_deployment.chat[0].model[0].version
    managed_identity_id        = azurerm_user_assigned_identity.agent[0].id
    managed_identity_client_id = azurerm_user_assigned_identity.agent[0].client_id
  }
}
