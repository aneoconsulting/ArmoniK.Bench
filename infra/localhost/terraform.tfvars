environment_name = "armonik-bench"

environment_version = "composer-2.7.1-airflow-2.7.3"

pypi_packages = {
  armonik                                  = "==3.17.0"
  apache-airflow-providers-cncf-kubernetes = ">=8.1.1"
  apache-airflow-providers-google          = ">=10.17.0"
  apache-airflow-providers-grpc            = "==3.5.0"
  boto3                                    = ">=1.34.129"
  polars                                   = "==0.20.24"
}

env_variables = {
  DAGS__RUN_EXPERIMENT__DEPLOY_WORKDIR = "/tmp/workdir"
  ARMONIK_BENCH__CONTEXT               = "local"
}
