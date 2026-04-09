output "raw_bucket_name" {
  description = "Name of the raw GCS bucket"
  value       = module.gcs.bucket_name
}

output "bigquery_dataset_ids" {
  description = "Map of managed BigQuery dataset IDs"
  value       = module.bigquery.dataset_ids
}

output "service_account_email" {
  description = "Email of the ingestion service account"
  value       = module.iam.service_account_email
}

output "sa_dbt_email" {
  description = "Email of the dbt service account"
  value       = google_service_account.sa_dbt.email
}

output "artifact_registry_repository" {
  description = "Full path of the Artifact Registry repository"
  # Usage: {region}-docker.pkg.dev/{project_id}/{repository_id}/{image_name}:{tag}
  value = "${google_artifact_registry_repository.docker.location}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.docker.repository_id}"
}

output "cloud_run_job_name" {
  description = "Name of the ingestion Cloud Run Job"
  value       = module.cloud_run.job_name
}

output "cloud_scheduler_name" {
  description = "Name of the Cloud Scheduler Job"
  value       = module.cloud_run.scheduler_name
}
