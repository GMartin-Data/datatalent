variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
}

variable "job_name" {
  description = "Name of the Cloud Run Job"
  type        = string
}

variable "image" {
  description = "Docker image URI (e.g. europe-west1-docker.pkg.dev/project/repo/image:tag)"
  type        = string
}

variable "service_account_email" {
  description = "Service account email for job execution and scheduler invocation"
  type        = string
}

variable "memory" {
  description = "Memory limit for the job container"
  type        = string
  default     = "4Gi"
}

variable "cpu" {
  description = "CPU limit for the job container"
  type        = string
  default     = "1"
}

variable "timeout" {
  description = "Maximum execution time in seconds"
  type        = string
  default     = "1800s"
}

variable "max_retries" {
  description = "Maximum number of retries on failure"
  type        = number
  default     = 0
}

variable "schedule" {
  description = "Cron schedule expression"
  type        = string
}

variable "scheduler_timezone" {
  description = "Timezone for the scheduler"
  type        = string
  default     = "Europe/Paris"
}

variable "secret_env_vars" {
  description = "Map of environment variable name to secret ID (e.g. FT_CLIENT_ID => projects/.../secrets/ft-client-id)"
  type        = map(string)
}
