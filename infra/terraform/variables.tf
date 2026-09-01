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
