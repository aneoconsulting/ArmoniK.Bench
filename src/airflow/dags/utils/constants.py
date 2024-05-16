from datetime import datetime, timedelta, timezone

# Default options for Airflow DAGs
DAG_DEFAULT_START_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)
DAG_DEFAULT_END_DATE = None
DAG_DEFAULT_MAX_ACTIVE_TASKS = 10
DAG_DEFAULT_MAX_ACTIVE_RUNS = 1
DAG_DEFAULT_DAGRUN_TIMEMOUT = timedelta(hours=5)
DAG_DEFAULT_ARGS_OWNER = "airflow"
DAG_DEFAULT_ARGS_RETRIES = 2
DAG_DEFAULT_ARGS_RETRY_DELAY = timedelta(seconds=10)

# Cloud Composer-specific constants
COMPOSER_CORE_DATA_DIR = "/home/airflow/gcs/data"
COMPOSER_K8S_USER_WORKLOAD_NAMESPACE = "composer-user-workloads"
COMPOSER_K8S_KUBECONFIG = "/home/airflow/composer_kube_config"
COMPOSER_K8S_CONN_ID = "kubernetes_default"

# User persistent volume options
USER_PVC_NAME = "pvc-workdir"
USER_PV_MOUNT_NAME = "pvc-workdir-vol"
USER_PV_MOUNT_PATH = "/tmp/workdir"

# Terraform
TF_IMAGE = "hashicorp/terraform:1.8"
TF_WORKDIR = (
    f"{USER_PV_MOUNT_PATH}/ArmoniK/infrastructure/quick-deploy/" "{{ var.value.environment_type }}"
)
TF_DATA_DIR = (f"{TF_WORKDIR}/generated",)
TF_PLUGIN_CACHE_DIR = (f"{TF_DATA_DIR}/terraform-plugins",)
TF_BACKEND_BUCKET = "armonik-bench-tfstate-{{ var.value.environment_type }}"
TF_BACKEND_STATE_FILE = "armonik-{{ var.value.environment_type }}.tfstate"
TF_PARAMETERS_FILE = (f"{TF_WORKDIR}/parameters.tfvars.json",)
TF_VERSIONS_FILE = (f"{USER_PV_MOUNT_PATH}/ArmoniK/versions.tfvars.json",)
TF_EXTRA_PARAMETERS_FILE = (f"{USER_PV_MOUNT_PATH}/ArmoniK/extra.tfvars.json",)
