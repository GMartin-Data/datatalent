output "dataset_ids" {
  description = "Map of managed BigQuery dataset IDs"
  value       = { for k, v in google_bigquery_dataset.this : k => v.dataset_id }
}
