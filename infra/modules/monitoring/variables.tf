variable "project_id" {
  description = "GCP project ID hosting the monitored resources and the alert policies"
  type        = string
}

variable "notification_email" {
  description = "Email address (or mailing list) receiving alert notifications"
  type        = string
}

variable "ingestion_job_name" {
  description = "Name of the ingestion Cloud Run Job to monitor for execution failures"
  type        = string
}

variable "dbt_job_name" {
  description = "Name of the dbt Cloud Run Job to monitor for execution failures"
  type        = string
}
