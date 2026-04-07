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
