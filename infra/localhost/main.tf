resource "local_file" "requirements_txt" {
  filename = local.requirements_source_path
  content  = join("\n", [for key, value in var.environment.pypi_packages : "${key}${value}"])
}

resource "docker_container" "composer_local_environment" {
  name        = var.environment.name
  image       = local.image_tag
  restart     = "always"
  entrypoint  = ["sh", local.entrypoint_target_path]
  env         = concat(local.composer_default_env_vars, local.user_env_vars)
  memory      = 4096
  working_dir = local.airflow_home
  ports {
    internal = 8080
    external = var.environment.airflow_ui_port
  }
  mounts {
    type   = "bind"
    target = local.dags_target_path
    source = local.dags_source_path
  }
  mounts {
    type   = "bind"
    target = local.plugins_target_path
    source = local.plugins_source_path
  }
  mounts {
    type   = "bind"
    target = local.tests_target_path
    source = local.tests_source_path
  }
  mounts {
    type   = "bind"
    target = local.gcloud_config_target_path
    source = local.gcloud_config_source_path
  }
  mounts {
    type   = "bind"
    target = local.kubeconfig_target_path
    source = local.kubeconfig_source_path
  }
  mounts {
    type   = "bind"
    target = local.entrypoint_target_path
    source = local.entrypoint_source_path
  }
  mounts {
    type   = "bind"
    target = local.requirements_target_path
    source = local.requirements_source_path
  }
}

data "generic_local_cmd" "home" {
  read "path" {
    cmd = "echo $HOME"
  }
}

data "generic_local_cmd" "whoami" {
  read "username" {
    cmd = "whoami"
  }
}

data "generic_local_cmd" "id" {
  read "uid" {
    cmd = "id -u"
  }
}

data "generic_local_cmd" "gcloud_project" {
  read "project" {
    cmd = "gcloud config get project"
  }
}

locals {
  project = coalesce(var.project, data.generic_local_cmd.gcloud_project.outputs.project)

  airflow_v  = replace(regex("composer-([1-9]+\\.[0-9]+\\.[0-9]+)-airflow-([1-9]+[\\.|-][0-9]+[\\.|-][0-9]+)", var.environment.image_version)[1], ".", "-")
  composer_v = regex("composer-([1-9]+\\.[0-9]+\\.[0-9]+)-airflow-([1-9]+[\\.|-][0-9]+[\\.|-][0-9]+)", var.environment.image_version)[0]
  image_tag  = "us-docker.pkg.dev/cloud-airflow-releaser/airflow-worker-scheduler-${local.airflow_v}/airflow-worker-scheduler-${local.airflow_v}:composer-${local.composer_v}-airflow-${local.airflow_v}"

  user_home_path            = data.generic_local_cmd.home.outputs.path
  tf_root_path              = abspath(path.root)
  project_root_path         = dirname(dirname(local.tf_root_path))
  dags_source_path          = "${local.project_root_path}/${var.environment.dags_path}"
  plugins_source_path       = "${local.project_root_path}/${var.environment.plugins_path}"
  tests_source_path         = "${local.project_root_path}/${var.environment.tests_path}"
  kubeconfig_source_path    = "${local.user_home_path}/${var.kubeconfig_path}"
  gcloud_config_source_path = "${local.user_home_path}/${var.gcloud_config_path}"
  entrypoint_source_path    = "${local.tf_root_path}/entrypoint.sh"
  requirements_source_path  = "${local.tf_root_path}/composer_requirements.txt"

  airflow_home              = "/home/airflow"
  dags_target_path          = "${local.airflow_home}/gcs/dags"
  plugins_target_path       = "${local.airflow_home}/gcs/plugins"
  tests_target_path         = "${local.airflow_home}/gcs/tests"
  kubeconfig_target_path    = "${local.airflow_home}/composer_kubeconfig"
  gcloud_config_target_path = "${local.airflow_home}/.config/gcloud"
  data_target_path          = "${local.airflow_home}/gcs/data"
  entrypoint_target_path    = "${local.airflow_home}/entrypoint.sh"
  requirements_target_path  = "${local.airflow_home}/composer_requirements.txt"

  composer_default_env_vars = [
    "AIRFLOW__API__AUTH_BACKEND=airflow.api.auth.backend.default",
    "AIRFLOW__WEBSERVER__EXPOSE_CONFIG=true",
    "AIRFLOW__CORE__LOAD_EXAMPLES=false",
    "AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=10",
    "AIRFLOW__CORE__DAGS_FOLDER=${local.dags_target_path}",
    "AIRFLOW__CORE__PLUGINS_FOLDER=${local.plugins_target_path}",
    "AIRFLOW__CORE__DATA_FOLDER=${local.data_target_path}",
    "AIRFLOW__WEBSERVER__RELOAD_ON_PLUGIN_CHANGE=true",
    "COMPOSER_PYTHON_VERSION=3",
    # By default, the container runs as the user `airflow` with UID 999. Set
    # this env variable to "True" to make it run as the current host user.
    "COMPOSER_CONTAINER_RUN_AS_HOST_USER=False",
    "COMPOSER_HOST_USER_NAME=${data.generic_local_cmd.whoami.outputs.username}",
    "COMPOSER_HOST_USER_ID=${data.generic_local_cmd.id.outputs.uid}",
    "AIRFLOW_HOME=${local.airflow_home}/airflow",
    "AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT=google-cloud-platform://?extra__google_cloud_platform__project=${local.project}&extra__google_cloud_platform__scope=https://www.googleapis.com/auth/cloud-platform",
  ]

  user_env_vars = [for key, value in var.environment.env_variables : "${key}=${value}"]
}
