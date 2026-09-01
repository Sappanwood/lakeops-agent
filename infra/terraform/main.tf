locals {
  base_name = "${var.resource_prefix}-${var.environment}"

  common_tags = {
    environment = var.environment
    managed_by  = "terraform"
    project     = "lakeops-agent"
  }
}

resource "azurerm_resource_group" "main" {
  name     = "rg-${local.base_name}"
  location = var.location
  tags     = local.common_tags
}
