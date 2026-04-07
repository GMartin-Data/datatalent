variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "service_account_id" {
  description = "ID of the service account (without @domain)"
  type        = string
}

variable "iam_members" {
  description = "List of IAM members (serviceAccount:, user:) to grant roles to"
  type        = list(string)
}

variable "roles" {
  description = "List of roles to grant to each member"
  type        = list(string)
}
