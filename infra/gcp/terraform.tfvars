environment = {
  name                 = "armonik-bench"
  image                = "composer-2.7.1-airflow-2.7.3"
  service_account_name = "airflow-bench-armonik"
  pypi_packages = {
    apache-airflow-providers-google          = ">=10.17.0"
    apache-airflow-providers-cncf-kubernetes = ">=8.1.1"
  }
  env_variables = {
    DAGS__RUN_EXPERIMENT__DEPLOY_WORKDIR = "/tmp/workdir"
  }
}
