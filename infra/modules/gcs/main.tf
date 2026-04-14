resource "google_storage_bucket" "raw" {
  project  = var.project_id
  name     = var.bucket_name
  location = var.region

  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "inherited"

  soft_delete_policy {
    retention_duration_seconds = 604800
  }

  # Purge des fichiers raw > 90 jours pour éviter l'accumulation
  # de snapshots obsolètes (Sirene ~2.5 GB/mois). BigQuery fait
  # foi après chargement. Voir D68.
  lifecycle_rule {
    condition {
      age = 90 # jours
    }
    action {
      type = "Delete"
    }
  }
}
