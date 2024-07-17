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

variable "environment_name" {
  description = "Cloud Composer local environment name."
  type        = string
  default     = "armonik-bench"
}

variable "environment_version" {
  description = "Cloud Composer local environment version"
  type        = string
  default     = "composer-2.7.0-airflow-2.7.3"
}

variable "environment_size" {
  description = "Cloud Composer environment size"
  type        = string
  default     = "ENVIRONMENT_SIZE_SMALL"
}

variable "environment_service_account" {
  description = "Name of the service account associated to the Cloud Composer environment"
  type        = string
}

variable "pypi_packages" {
  description = "PyPi packages to install in the Cloud Composer local environment"
  type        = map(string)
  default     = {}
}

variable "env_variables" {
  description = "Environment variable to add to the Cloud Composer local environment"
  type        = map(string)
  default     = {}
}

variable "kubeconfig_path" {
  description = "Path to save the kubeconfig file."
  type        = string
  default     = "generated/composer_gke_kubeconfig"
}
