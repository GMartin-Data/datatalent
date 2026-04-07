resource "google_service_account" "this" {
  project      = var.project_id
  account_id   = var.service_account_id
  display_name = "Service Account Ingestion"
}

locals {
  member_role_pairs = flatten([
    for member in var.iam_members : [
      for role in var.roles : {
        member = member
        role   = role
      }
    ]
  ])
}

resource "google_project_iam_member" "this" {
  for_each = {
    for pair in local.member_role_pairs : "${pair.member}-${pair.role}" => pair
  }
  project = var.project_id
  role    = "roles/${each.value.role}"
  member  = each.value.member
}
