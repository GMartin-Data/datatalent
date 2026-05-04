resource "google_secret_manager_secret" "this" {
  for_each = toset(var.secret_names)

  project   = var.project_id
  secret_id = each.key

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "this" {
  for_each = toset(var.secret_names)

  secret      = google_secret_manager_secret.this[each.key].id
  secret_data = var.secret_values[each.key]
}
