module "gcs" {
  source = "./modules/gcs"

  project_id  = var.project_id
  bucket_name = "datatalent-glaq-2-raw"
  region      = var.region
}

module "bigquery" {
  source = "./modules/bigquery"

  project_id  = var.project_id
  region      = var.region
  dataset_ids = ["raw", "staging", "intermediate", "marts"]
}

module "iam" {
  source = "./modules/iam"

  project_id         = var.project_id
  service_account_id = "sa-ingestion"
  iam_members = [
    "serviceAccount:sa-ingestion@${var.project_id}.iam.gserviceaccount.com",
    "user:gregory.martin.data@gmail.com",
    "user:kent1.esnault@gmail.com",
    "user:abdel.daadi.pro@gmail.com",
    "user:louismoises987@gmail.com",
  ]
  roles = [
    "storage.objectAdmin",
    "bigquery.dataEditor",
    "bigquery.jobUser",
    "secretmanager.secretAccessor",
  ]
}

resource "google_service_account" "sa_dbt" {
  project      = var.project_id
  account_id   = "sa-dbt"
  display_name = "Service Account dbt"
}

resource "google_project_iam_member" "sa_dbt" {
  for_each = toset([
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
  ])

  project = var.project_id
  role    = each.value
  # Computed attribute: resolved by the provider from account_id + project
  member = "serviceAccount:${google_service_account.sa_dbt.email}"
}

resource "google_project_service" "apis" {
  for_each = toset([
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "secretmanager.googleapis.com",
    "artifactregistry.googleapis.com",
  ])

  project = var.project_id
  service = each.value

  disable_on_destroy = false
}

resource "google_artifact_registry_repository" "docker" {
  project       = var.project_id
  location      = var.region
  repository_id = "datatalent"
  format        = "DOCKER"
  description   = "Docker images for Datatalent pipelines"

  # Ensure the API is enabled before creating the repository
  depends_on = [google_project_service.apis]
}