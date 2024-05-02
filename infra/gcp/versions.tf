terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.26.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.21.1"
    }
    generic = {
      source  = "aneoconsulting.github.io/aneoconsulting/generic"
      version = "~> 0.1.0"
    }
  }
}
