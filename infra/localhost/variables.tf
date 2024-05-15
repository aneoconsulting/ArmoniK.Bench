# GCP project
variable "project" {
  description = "GCP project name"
  type        = string
  default     = null
}

variable "environment" {
  description = "Cloud Composer local environment configuration"
  type = object({
    name            = optional(string, "armonik-bench")
    image_version   = optional(string, "composer-2.7.0-airflow-2.7.3")
    pypi_packages   = optional(map(string), {})
    env_variables   = optional(map(string), {})
    airflow_ui_port = optional(number, 8080)
    dags_path       = optional(string, "src/airflow/dags")
    plugins_path    = optional(string, "src/airflow/plugins")
    tests_path      = optional(string, "tests")
  })
}

variable "kubeconfig_path" {
  description = "Path to local kubernetes cluster credentials."
  type        = string
  default     = ".kube/config"
}

variable "gcloud_config_path" {
  description = "Path to local gcloud credentials."
  type        = string
  default     = ".config/gcloud"
}
