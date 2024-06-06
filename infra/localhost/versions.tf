terraform {
  required_version = ">= 1.6"
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0.2"
    }
    generic = {
      source  = "aneoconsulting.github.io/aneoconsulting/generic"
      version = "~> 0.1.0"
    }
  }
}
