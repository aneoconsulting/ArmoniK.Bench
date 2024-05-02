terraform {
  backend "gcs" {
    bucket = "armonik-bench-composer"
    prefix = "armonik-bench-terraform.tfstate"
  }
}
