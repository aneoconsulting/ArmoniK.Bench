resource "google_composer_environment" "composer_environment" {
  name    = var.environment.name
  project = local.project
  region  = local.region

  config {
    environment_size = var.environment.size
    software_config {
      image_version = var.environment.image
      pypi_packages = var.environment.pypi_packages
      env_variables = var.environment.env_variables
    }

    node_config {
      service_account = "${var.environment.service_account_name}@${local.project}.iam.gserviceaccount.com"
    }
  }
}

resource "kubernetes_persistent_volume_claim" "pvc_workdir" {
  depends_on       = [google_composer_environment.composer_environment]
  wait_until_bound = false
  metadata {
    name      = "pvc-workdir"
    namespace = "composer-user-workloads"
  }

  spec {
    access_modes = ["ReadWriteOnce"]

    resources {
      requests = {
        storage = "1Gi"
      }
    }

    storage_class_name = "standard-rwo"
  }
}

resource "generic_local_cmd" "kubeconfig" {
  inputs = {
    cluster    = local.cluster_name
    project    = local.project
    location   = local.region
    kubeconfig = var.kubeconfig_path
  }
  create {
    cmd = <<-EOT
      KUBECONFIG="$INPUT_kubeconfig" \
      gcloud container clusters get-credentials "$INPUT_cluster" \
        --location "$INPUT_location" \
        --project "$INPUT_project"
    EOT
  }
  destroy {
    cmd = <<-EOT
      rm -f "$INPUT_kubeconfig"
    EOT
  }
}

# Synchronises local DAG files with those in the Cloud Composer environment.
resource "google_storage_bucket_object" "python_files" {
  for_each = { for dag_file in local.dag_files : dag_file => dag_file }

  name   = "dags/${each.value}"
  bucket = local.bucket_name
  source = "${local.dags_path}/${each.value}"
}
