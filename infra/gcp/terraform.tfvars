environment = {
  name                 = "armonik-bench"
  image                = "composer-2.7.0-airflow-2.7.3"
  service_account_name = "airflow-bench-armonik"
  pypi_packages        = {}
  env_variables = {
    DAGS__RUN_EXPERIMENT__DEPLOY_WORKDIR = "/tmp/workdir"
  }
}
