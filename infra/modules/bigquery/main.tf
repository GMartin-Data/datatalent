resource "google_bigquery_dataset" "this" {
  for_each = toset(var.dataset_ids)

  project    = var.project_id
  dataset_id = each.value
  location   = var.region
}
