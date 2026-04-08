output "service_account_email" {
  description = "Email of the managed service account"
  # .email is a computed attribute resolved by GCP after import
  value = google_service_account.this.email
}
