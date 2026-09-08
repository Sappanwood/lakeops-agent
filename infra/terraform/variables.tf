variable "subscription_id" {
  description = "Azure subscription used for the portfolio environment."
  type        = string
}

variable "location" {
  description = "Primary Azure region."
  type        = string
  default     = "japaneast"
}

variable "environment" {
  description = "Deployment environment name."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "demo"], var.environment)
    error_message = "Environment must be dev or demo."
  }
}

variable "resource_prefix" {
  description = "Prefix used for Azure resource names."
  type        = string
  default     = "lakeops"
}

variable "foundry" {
  description = "Opt-in Foundry model resources. Null keeps the resource-group-only environment. Capacity is in thousands of tokens per minute, not a spending cap."
  type = object({
    account_name = string
    capacity     = optional(number, 10)
  })
  default = null

  validation {
    condition     = var.foundry == null ? true : can(regex("^[a-z][a-z0-9-]{1,61}[a-z0-9]$", var.foundry.account_name))
    error_message = "Foundry account_name must be a 3-63 character lowercase DNS label starting with a letter and ending with a letter or digit."
  }

  validation {
    condition     = var.foundry == null ? true : var.foundry.capacity >= 1 && var.foundry.capacity <= 10 && floor(var.foundry.capacity) == var.foundry.capacity
    error_message = "Foundry capacity must be an integer from 1 through 10 (1,000-10,000 TPM)."
  }
}
