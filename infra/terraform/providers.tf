provider "azapi" {
  subscription_id = var.subscription_id
}

provider "azurerm" {
  features {
    cognitive_account {
      purge_soft_delete_on_destroy = false
    }
  }

  subscription_id                 = var.subscription_id
  resource_provider_registrations = "none"
}
