# GCP region
variable "region" {
  description = "The GCP region used to deploy all resources"
  type        = string
  default     = "europe-west1"
}

# GCP project
variable "project" {
  description = "GCP project name"
  type        = string
  default     = null
}

variable "environment" {
  description = "Cloud Composer environment configuration"
  type = object({
    name                 = optional(string, "armonik-bench")
    image                = optional(string, "composer-2.6.6-airflow-2-7-3")
    size                 = optional(string, "ENVIRONMENT_SIZE_SMALL")
    service_account_name = string
    pypi_packages        = optional(map(string), {})
    env_variables        = optional(map(string), {})
  })
}

variable "kubeconfig_path" {
  description = "Path to save the kubeconfig file."
  type        = string
  default     = "generated/composer_gke_kubeconfig"
}
