output "notification_channel_id" {
  description = "Full ID of the email notification channel. Consumed internally by the alert policies of this module; also exposed for inspection (e.g. T13.5 incident test, manual checks via gcloud or console)."
  value       = google_monitoring_notification_channel.email.id
}
