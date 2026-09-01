output "resource_group_id" {
  description = "ID of the LakeOps Agent resource group."
  value       = azurerm_resource_group.main.id
}

output "resource_group_name" {
  description = "Name of the LakeOps Agent resource group."
  value       = azurerm_resource_group.main.name
}
