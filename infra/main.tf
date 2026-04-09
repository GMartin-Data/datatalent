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
    "run.googleapis.com",
    "cloudscheduler.googleapis.com",
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

module "secret_manager" {
  source = "./modules/secret_manager"

  project_id   = var.project_id
  secret_names = ["ft-client-id", "ft-client-secret", "adzuna-app-id", "adzuna-app-key"]
  secret_values = {
    "ft-client-id"     = var.ft_client_id
    "ft-client-secret" = var.ft_client_secret
    "adzuna-app-id"    = var.adzuna_app_id
    "adzuna-app-key"   = var.adzuna_app_key
  }

  depends_on = [google_project_service.apis]
}

module "cloud_run" {
  source = "./modules/cloud_run"

  project_id            = var.project_id
  region                = var.region
  job_name              = "datatalent-ingestion"
  image                 = "${google_artifact_registry_repository.docker.location}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.docker.repository_id}/ingestion:initial"
  service_account_email = module.iam.service_account_email
  schedule              = "0 6 * * 1"

  secret_env_vars = {
    FT_CLIENT_ID     = module.secret_manager.secret_ids["ft-client-id"]
    FT_CLIENT_SECRET = module.secret_manager.secret_ids["ft-client-secret"]
    ADZUNA_APP_ID    = module.secret_manager.secret_ids["adzuna-app-id"]
    ADZUNA_APP_KEY   = module.secret_manager.secret_ids["adzuna-app-key"]
  }

  depends_on = [google_project_service.apis]
}
