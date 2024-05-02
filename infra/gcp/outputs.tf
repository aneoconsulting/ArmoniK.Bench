output "airflow_ui_uri" {
  description = "The URI of the Apache Airflow Web UI hosted within the composer environment"
  value       = google_composer_environment.composer_environment.config.0.airflow_uri
}


output "kubeconfig" {
  description = "Path to the kubeconfig file to access the Cloud Composer Kubernetes cluster"
  value       = abspath(var.kubeconfig_path)
}
