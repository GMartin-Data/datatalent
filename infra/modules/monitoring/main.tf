resource "google_monitoring_notification_channel" "email" {
  project      = var.project_id
  display_name = "DataTalent alerts (email)"
  type         = "email"

  labels = {
    email_address = var.notification_email
  }
}

resource "google_monitoring_alert_policy" "ingestion_job_failed" {
  project      = var.project_id
  display_name = "Ingestion job execution failed"
  combiner     = "OR"

  notification_channels = [google_monitoring_notification_channel.email.id]

  conditions {
    display_name = "Failed execution count > 0"

    condition_threshold {
      filter = join(" AND ", [
        "metric.type=\"run.googleapis.com/job/completed_execution_count\"",
        "resource.type=\"cloud_run_job\"",
        "resource.label.\"job_name\"=\"${var.ingestion_job_name}\"",
        "metric.label.\"result\"=\"failed\"",
      ])
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      # 60s = shortest duration accepted by the API alongside evaluation_missing_data.
      duration = "60s"

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_SUM"
      }

      # Between weekly runs the metric is absent; without this, the alert would
      # stay permanently open on missing data.
      evaluation_missing_data = "EVALUATION_MISSING_DATA_INACTIVE"
    }
  }
}

resource "google_monitoring_alert_policy" "dbt_job_failed" {
  project      = var.project_id
  display_name = "dbt job execution failed"
  combiner     = "OR"

  notification_channels = [google_monitoring_notification_channel.email.id]

  conditions {
    display_name = "Failed execution count > 0"

    condition_threshold {
      filter = join(" AND ", [
        "metric.type=\"run.googleapis.com/job/completed_execution_count\"",
        "resource.type=\"cloud_run_job\"",
        "resource.label.\"job_name\"=\"${var.dbt_job_name}\"",
        "metric.label.\"result\"=\"failed\"",
      ])
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "60s"

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_SUM"
      }

      evaluation_missing_data = "EVALUATION_MISSING_DATA_INACTIVE"
    }
  }
}

