resource "docker_container" "db" {
  name         = local.db_container_name
  image        = "postgres:${var.db_version}"
  restart      = "always"
  # network_mode = "host"
  ports {
    internal = 5432
    external = 3225
  }
  memory       = 128
  env = [
    "POSTGRES_USER=${var.db_user}",
    "POSTGRES_PASSWORD=${var.db_password}",
    "POSTGRES_DB=${var.db_name}",
  ]
}


resource "docker_container" "composer_local_environment" {
  name        = var.environment_name
  image       = local.image_tag
  restart     = "always"
  entrypoint  = ["sh", "-c", "mkdir -p ${local.airflow_home}/airflow && sh ${local.entrypoint_target_path}"]
  env         = concat(local.composer_default_env_vars, local.user_env_vars)
  memory      = 4096
  working_dir = local.airflow_home
  # ports {
  #   internal = 8080
  #   external = var.airflow_ui_port
  # }
  network_mode = "host"
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
  upload {
    file   = local.entrypoint_target_path
    source = local.entrypoint_source_path
  }
  upload {
    file    = local.requirements_target_path
    content = join("\n", [for key, value in var.pypi_packages : "${key}${value}"])
  }
}

resource "kubernetes_namespace" "k8s_namespace" {
  metadata {
    name = local.k8s_namespace
  }
}

resource "kubernetes_service_account" "terraform_sa" {
  metadata {
    name      = "terraform-sa"
    namespace = local.k8s_namespace
  }
}

resource "kubernetes_cluster_role" "terraform_cr" {
  metadata {
    name = "terraform-cr"
  }

  rule {
    api_groups = ["*"]
    resources  = ["*"]
    verbs      = ["*"]
  }
}

resource "kubernetes_cluster_role_binding" "terraform_crb" {
  metadata {
    name = "terraform-crb"
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.terraform_cr.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.terraform_sa.metadata[0].name
    namespace = local.k8s_namespace
  }
}

resource "kubernetes_persistent_volume" "pv_workdir" {
  metadata {
    name = "pv-workdir"
  }

  spec {
    access_modes = ["ReadWriteOnce"]

    capacity = {
      storage = "1Gi"
    }

    persistent_volume_source {
      host_path {
        path = "/tmp/workdir"
        type = "DirectoryOrCreate"
      }
    }

    persistent_volume_reclaim_policy = "Delete"

    storage_class_name = "local-path"

  }
}

resource "kubernetes_persistent_volume_claim" "pvc_workdir" {
  depends_on       = [kubernetes_persistent_volume.pv_workdir]
  wait_until_bound = true
  metadata {
    name      = "pvc-workdir"
    namespace = local.k8s_namespace
  }

  spec {
    access_modes = ["ReadWriteOnce"]

    resources {
      requests = {
        storage = "1Gi"
      }
    }

    volume_name = kubernetes_persistent_volume.pv_workdir.metadata[0].name
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

data "generic_local_cmd" "aws_access_key_id" {
  read "secret" {
    cmd = "gcloud secrets versions access 1 --secret=armonik-bench-aws-access-key"
  }
}

data "generic_local_cmd" "aws_secret_access_key" {
  read "secret" {
    cmd = "gcloud secrets versions access 1 --secret=armonik-bench-aws-secret-access-key"
  }
}

# data "google_secret_manager_secret_version" "aws_access_key_id" {
#   secret  = "armonik-bench-aws-access-key"
#   project = local.project
# }

# data "google_secret_manager_secret_version" "aws_secret_access_key" {
#   secret  = "armonik-bench-aws-secret-access-key"
#   version = "latest"
#   project = local.project
# }

locals {
  project = coalesce(var.project, data.generic_local_cmd.gcloud_project.outputs.project)

  db_container_name = "${var.environment_name}-db"

  version_pattern = "composer-([1-9]+\\.[0-9]+\\.[0-9]+)-airflow-([1-9]+[\\.|-][0-9]+[\\.|-][0-9]+)"
  airflow_v       = replace(regex(local.version_pattern, var.environment_version)[1], ".", "-")
  composer_v      = regex(local.version_pattern, var.environment_version)[0]
  image_tag       = "us-docker.pkg.dev/cloud-airflow-releaser/airflow-worker-scheduler-${local.airflow_v}/airflow-worker-scheduler-${local.airflow_v}:composer-${local.composer_v}-airflow-${local.airflow_v}"

  user_home_path            = data.generic_local_cmd.home.outputs.path
  tf_root_path              = abspath(path.root)
  project_root_path         = dirname(dirname(local.tf_root_path))
  dags_source_path          = "${local.project_root_path}/${var.dags_path}"
  plugins_source_path       = "${local.project_root_path}/${var.plugins_path}"
  tests_source_path         = "${local.project_root_path}/${var.tests_path}"
  kubeconfig_source_path    = "${local.user_home_path}/${var.kubeconfig_path}"
  gcloud_config_source_path = "${local.user_home_path}/${var.gcloud_config_path}"
  entrypoint_source_path    = "${local.tf_root_path}/composer-local-dev/composer_local_dev/docker_files/entrypoint.sh"

  airflow_home              = "/home/airflow"
  dags_target_path          = "${local.airflow_home}/gcs/dags"
  plugins_target_path       = "${local.airflow_home}/gcs/plugins"
  tests_target_path         = "${local.airflow_home}/gcs/tests"
  kubeconfig_target_path    = "${local.airflow_home}/composer_kube_config"
  gcloud_config_target_path = "${local.airflow_home}/.config/gcloud"
  data_target_path          = "${local.airflow_home}/gcs/data"
  entrypoint_target_path    = "${local.airflow_home}/entrypoint.sh"
  requirements_target_path  = "${local.airflow_home}/composer_requirements.txt"
  db_target_path            = "${local.airflow_home}/airflow/airflow.db"

  composer_default_env_vars = [
    "AIRFLOW__API__AUTH_BACKEND=airflow.api.auth.backend.default",
    "AIRFLOW__WEBSERVER__EXPOSE_CONFIG=true",
    "AIRFLOW__CORE__LOAD_EXAMPLES=false",
    "AIRFLOW__CORE__EXECUTOR=LocalExecutor",
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://${var.db_user}:${var.db_password}@127.0.0.1:3225/${var.db_name}",
    "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False",
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
    # AWS credentials
    "AWS_ACCESS_KEY_ID=${data.generic_local_cmd.aws_access_key_id.outputs.secret}",
    "AWS_SECRET_ACCESS_KEY=${data.generic_local_cmd.aws_secret_access_key.outputs.secret}",
  ]

  user_env_vars = [for key, value in var.env_variables : "${key}=${value}"]
  k8s_namespace = "composer-user-workloads"
}
