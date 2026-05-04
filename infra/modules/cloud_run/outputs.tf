output "job_name" {
  description = "Name of the Cloud Run Job"
  value       = google_cloud_run_v2_job.this.name
}

output "scheduler_name" {
  description = "Name of the Cloud Scheduler Job (null if create_scheduler = false)"
  value       = try(google_cloud_scheduler_job.this[0].name, null)
}
