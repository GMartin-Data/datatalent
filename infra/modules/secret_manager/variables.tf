variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "secret_names" {
  description = "List of secret names to create"
  type        = list(string)
}

variable "secret_values" {
  description = "Map of secret name to secret value"
  type        = map(string)
  sensitive   = true
}
