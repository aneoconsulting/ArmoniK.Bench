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
  bucket_name  = trimprefix(trimsuffix(google_composer_environment.composer_environment.config[0].dag_gcs_prefix, "/dags"), "gs://")
  # Define local variables to list DAG files.
  ignore_patterns = [".*__init__.py"]
  dags_path       = "${dirname(dirname(abspath(path.root)))}/src/airflow/dags"
  raw_dag_files   = fileset(local.dags_path, "**/*.py")
  dag_files = [
    for file in local.raw_dag_files : file if length([
      for pattern in local.ignore_patterns : file if length(regexall(pattern, file)) > 0
    ]) == 0
  ]
}
