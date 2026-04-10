variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "europe-west1"
}

variable "ft_client_id" {
  description = "France Travail OAuth2 client ID"
  type        = string
  sensitive   = true
}

variable "ft_client_secret" {
  description = "France Travail OAuth2 client secret"
  type        = string
  sensitive   = true
}

variable "adzuna_app_id" {
  description = "Adzuna API app ID"
  type        = string
  sensitive   = true
}

variable "adzuna_app_key" {
  description = "Adzuna API app key"
  type        = string
  sensitive   = true
}

variable "billing_account_id" {
  description = "GCP billing account ID for budget alerts and billing export"
  type        = string
  sensitive   = true
}
