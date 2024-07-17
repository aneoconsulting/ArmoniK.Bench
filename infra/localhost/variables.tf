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

variable "airflow_ui_port" {
  description = "Port used to expose the Airflow UI"
  type        = number
  default     = 8080
}

variable "dags_path" {
  description = "Relative path of the folder containing the DAG files in the project tree."
  type        = string
  default     = "src/airflow/dags"
}

variable "plugins_path" {
  description = "Relative path of the folder containing the Airflow plugins of the project in the project tree"
  type        = string
  default     = "src/airflow/plugins"
}

variable "tests_path" {
  description = "Relative path of the folder containing the tests of the project in the project tree"
  type        = string
  default     = "tests"
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

variable "db_version" {
  description = "Version of the official Postgres Docker image."
  type        = string
  default     = "latest"
}

variable "db_user" {
  description = "Database user name."
  type        = string
  default     = "airflow"
}

variable "db_password" {
  description = "Database user password."
  type        = string
  default     = "pass"
}

variable "db_name" {
  description = "Default database name."
  type        = string
  default     = "airflow_db"
}
