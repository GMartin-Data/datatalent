# =============================================================================
# DataTalent - Configuration Terraform racine
# Orchestre tous les modules et ressources standalone
# Voir modules/ pour le détail d'implémentation
# =============================================================================

# --- Stockage ---

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

# --- IAM ---
# Trois périmètres distincts, séparés selon le principe du moindre privilège:
#  1. module iam/         → sa-ingestion + 4 users × 4 rôles partagés (produit cartésien)
#  2. sa_dbt              → SA dédié avec rôles BigQuery uniquement
#  3. sa_ingestion_ci_cd  → rôles CI/CD réservés à sa-ingestion seul

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
    "serviceusage.serviceUsageConsumer",
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
  # Attribut computed : résolu par le provider à partir de account_id + project
  member = "serviceAccount:${google_service_account.sa_dbt.email}"
}

# Rôles CI/CD - hors module iam/ pour ne pas les attribuer aux 5 membres
resource "google_project_iam_member" "sa_ingestion_ci_cd" {
  for_each = toset([
    "roles/artifactregistry.writer", # push des images Docker
    "roles/run.developer",           # gcloud run jobs update
    "roles/iam.serviceAccountUser",  # actAs sa-ingestion pour Cloud Run
  ])

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${module.iam.service_account_email}"
}

# --- APIs GCP ---

resource "google_project_service" "apis" {
  for_each = toset([
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "secretmanager.googleapis.com",
    "artifactregistry.googleapis.com",
    "run.googleapis.com",
    "cloudscheduler.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "billingbudgets.googleapis.com",
    "iam.googleapis.com",
  ])

  project = var.project_id
  service = each.value

  disable_on_destroy = false
}

# --- Artifact Registry ---

resource "google_artifact_registry_repository" "docker" {
  project       = var.project_id
  location      = var.region
  repository_id = "datatalent"
  format        = "DOCKER"
  description   = "Docker images for Datatalent pipelines"

  # Purge automatique des anciennes images Docker pour rester
  # sous le free tier Artifact Registry (500 MB). Seules les 3
  # dernières versions de chaque image sont conservées. Voir D69.
  cleanup_policies {
    id     = "keep-recent"
    action = "KEEP"

    most_recent_versions {
      keep_count = 3
    }
  }

  # L'API doit être activée avant de créer le repository
  depends_on = [google_project_service.apis]
}

# --- Secret Manager ---
# Credentials des sources authentifiées (France Travail OAuth2, Adzuna API).
# Valeurs dans terraform.tfvars (gitignored), montées comme env vars dans Cloud Run.

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

# --- Compute: Cloud Run Job + Scheduler ---
# Pipeline d'ingestion - Exécution chaque lundi 6h (D19, D63)
# Le tag ":initial" est un placeholder de bootstrap: deploy.yml le remplace
# par :$GITHUB_SHA à chaque merge sur main

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

resource "google_bigquery_dataset" "billing_export" {
  dataset_id = "billing_export"
  location   = var.region

  description = "GCP billing export - detailed usage cost data"
}

resource "google_billing_budget" "project_budget" {
  billing_account = var.billing_account_id
  display_name    = "DataTalent - 10 EUR monthly budget"

  budget_filter {
    projects = ["projects/${var.project_id}"]
  }

  amount {
    specified_amount {
      currency_code = "EUR"
      units         = "10"
    }
  }

  threshold_rules {
    threshold_percent = 0.5
    spend_basis       = "CURRENT_SPEND"
  }

  threshold_rules {
    threshold_percent = 0.9
    spend_basis       = "CURRENT_SPEND"
  }

  threshold_rules {
    threshold_percent = 1.0
    spend_basis       = "CURRENT_SPEND"
  }

  depends_on = [google_project_service.apis]
}
