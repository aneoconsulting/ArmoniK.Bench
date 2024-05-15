terraform {
  backend "local" {
    path          = "composer-local-dev.tfstate"
    workspace_dir = "terraform_state"
  }
}
