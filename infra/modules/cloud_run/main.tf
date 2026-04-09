# --- 1. Le job Cloud Run ---

resource "google_cloud_run_v2_job" "this" {
  project  = var.project_id
  location = var.region
  name     = var.job_name

  template {
    task_count = 1

    template {
      max_retries     = var.max_retries
      service_account = var.service_account_email
      timeout         = var.timeout

      containers {
        image = var.image

        resources {
          limits = {
            memory = var.memory
            cpu    = var.cpu
          }
        }

        # Env vars statiques (si besoin futur)
        # env {
        #   name  = "LOG_LEVEL"
        #   value = "INFO"
        # }

        # Env vars depuis Secret Manager
        # Chaque entrée de la map crée un bloc env avec une référence
        # au secret. Cloud Run résout la valeur au démarrage du conteneur.
        dynamic "env" {
          for_each = var.secret_env_vars
          content {
            name = env.key
            value_source {
              secret_key_ref {
                secret  = env.value
                version = "latest"
              }
            }
          }
        }
      }
    }
  }

  # L'image est mise à jour par la CI/CD (deploy.yml) à chaque merge sur main.
  # client/client_version sont des métadonnées injectées par gcloud.
  # Terraform ne doit pas les réverter.
  lifecycle {
    ignore_changes = [
      template[0].template[0].containers[0].image,
      client,
      client_version,
    ]
  }
}

# --- 2. IAM : autoriser sa-ingestion à déclencher ce job ---
#
# Ce binding dit à Cloud Run :
#   "Le SA sa-ingestion a le rôle run.invoker SUR CE JOB uniquement."
#
# Sans ça, le Scheduler (qui s'authentifie en tant que sa-ingestion)
# recevrait un 403 Forbidden en essayant de lancer le job.

resource "google_cloud_run_v2_job_iam_member" "invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_job.this.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${var.service_account_email}"
}

# --- 3. Cloud Scheduler : cron qui déclenche le job ---
#
# Le Scheduler envoie un POST HTTP vers l'API Cloud Run Admin.
# Il signe la requête avec un token OAuth de sa-ingestion.
# Cloud Run vérifie le token → consulte le binding ci-dessus → autorise.

resource "google_cloud_scheduler_job" "this" {
  project   = var.project_id
  region    = var.region
  name      = "${var.job_name}-scheduler"
  schedule  = var.schedule
  time_zone = var.scheduler_timezone

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${var.job_name}:run"

    oauth_token {
      service_account_email = var.service_account_email
    }
  }
}
