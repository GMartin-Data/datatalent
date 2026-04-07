terraform {
  backend "gcs" {
    bucket = "datatalent-glaq-2-tfstate"
    prefix = "terraform/state"
  }
}
