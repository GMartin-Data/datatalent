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
