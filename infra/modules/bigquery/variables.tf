variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
}

variable "dataset_ids" {
  description = "List of BigQuery dataset IDs to manage"
  type        = list(string)
}
