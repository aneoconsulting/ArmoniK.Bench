output "composer_container_name" {
  description = "Name of the container hosting the local Cloud Composer environment"
  value       = var.environment_name
}

output "db_container_name" {
  description = "Name of the container hosting the local Postgres database"
  value       = local.db_container_name
}
