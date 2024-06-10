output "airflow_ui_uri" {
  description = "The URI of the Apache Airflow Web UI hosted within the composer environment"
  value       = "${google_composer_environment.composer_environment.config[0].airflow_uri}/home"
}


output "kubeconfig" {
  description = "Path to the kubeconfig file to access the Cloud Composer Kubernetes cluster"
  value       = abspath(var.kubeconfig_path)
}

output "cluster_name" {
  description = "Name of the GKE cluster hosting the Composer environment"
  value       = local.cluster_name
}

output "bucket_name" {
  description = "Name of the GCS bucket hosting DAG files"
  value       = local.bucket_name
}

output "dag_files" {
  description = "List of up-to-date DAG files in the Cloud Composer environment"
  value       = [for dag_file in local.dag_files : "${local.dags_path}/${dag_file}"]
}
