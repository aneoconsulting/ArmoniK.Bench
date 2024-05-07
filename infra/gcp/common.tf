# Retrieve an access token as the Terraform runner
data "google_client_config" "current" {}


data "google_container_cluster" "composer_cluster" {
  project  = local.project
  name     = local.cluster_name
  location = local.region
}

data "generic_local_cmd" "gcloud_project" {
  read "project" {
    cmd = "gcloud config get project"
  }
}


locals {
  project      = coalesce(var.project, data.generic_local_cmd.gcloud_project.outputs.project)
  region       = var.region
  cluster_name = split("/", google_composer_environment.composer_environment.config[0].gke_cluster)[5]
}
