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

  project_id = var.project_id
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