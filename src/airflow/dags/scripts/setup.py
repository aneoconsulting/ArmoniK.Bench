import json
import logging
import os
import pathlib
import shutil
import subprocess
import sys
import textwrap


def clone_repo(repo_path: pathlib.Path, repo_url: str, ref: str) -> None:
    """Clone a Git repository from an URL and checkout to a specific commit, branch, or tag.
    If the destination path already exists, it will be overwritten.

    Parameters
    ----------
    repo_path : pathlib.Pathlib.Path
        Path where to clone the repository.
    repo_url : str
        URL of the Git repository to clone.
    ref : str
        Reference to checkout (commit hash, branch name, or tag name).

    Raises
    ------
    ValueError
        If the 'repo_path' provided corresponds to an existing file.
    git.exc.GitCommandError
        If an error occurs when cloning the repository or checking out.
    """
    if repo_path.exists():
        if repo_path.is_dir():
            shutil.rmtree(repo_path)
        else:
            raise ValueError(f"{repo_path} is not a path to a directory.")

    # Cone repository using Git CLI
    subprocess.run(["git", "clone", repo_url, str(repo_path)], check=True)

    # Checkout repository using Git CLI
    subprocess.run(["git", "checkout", ref], check=True, cwd=str(repo_path))


def edit_default_parameters_file(repo_path: pathlib.Path, environment: str, config: dict) -> None:
    """Edit the default parameters file for the Terraform ArmoniK deployment.

    This function takes a repository path, an environment name, and a dictionary
    of configuration parameters. It writes the configuration to a Terraform
    parameters file and renames it with a '.json' extension.

    Parameters
    ----------
    repo_path : pathlib.Path
        Path to a local clone of the repository hosting the ArmoniK project.
    environment : str
        Deployment environment ('locahost', 'aws', 'gcp').
    config : dict
        Configuration for deployment (the contents of the parameter file that will be written).


    Raises
    ------
    TypeError
        If the 'config' provided is not JSON-serializable.
    FileNotFoundError
        If the 'repo_path' doesn't correspond to a local clone of the ArmoniK repository.
    """
    parameters_file = repo_path / f"infrastructure/quick-deploy/{environment}/parameters.tfvars"

    with parameters_file.open("w") as f:
        f.write(json.dumps(config))

    parameters_file.rename(parameters_file.with_suffix(".tfvars.json"))


def swap_terraform_backend_and_providers(repo_path: pathlib.Path, environment: str, context: str) -> None:
    """Swap default Terraform backend and providers to enable deployment from within a Kubernetes pod.
    
    Parameters
    ----------
    repo_path : pathlib.Path
        Path to a local clone of the repository hosting the ArmoniK project.
    environment : str
        Deployment environment ('locahost', 'aws', 'gcp').
    context : str
        Weither the ArmoniK Bench application is running in development environment or in production.
    """
    if context == "local":
        backend_data = """
            terraform {
                backend "kubernetes" {
                    secret_suffix    = "armonik-terraform.tfstate"
                    in_cluster_config = true
                }
            }
        """
    else:
        backend_data = """
            terraform {
                backend "gcs" {
                    prefix = "armonik-terraform.tfstate"
                    bucket = "airflow-bench-tfstate"
                }
            }
        """

    with (repo_path / f"infrastructure/quick-deploy/{environment}/backend.tf").open("w") as f:
        f.write(textwrap.dedent(backend_data))
    (repo_path / f"infrastructure/quick-deploy/{environment}/providers.tf").unlink(missing_ok=False)


def download_terraform_modules(repo_path: pathlib.Path, environment: str) -> None:
    """Download ArmoniK Terraform modules based on the configuration provided in the 'versions'
    file of ArmoniK repository already cloned.

        Parameters
    ----------
    repo_path : pathlib.Pathlib.Path
        Path where to download the modules.
    environment : str
        Deployment environment ('locahost', 'aws', 'gcp').

    """
    with (repo_path / "versions.tfvars.json").open() as f:
        data = json.loads(f.read())
        modules_source = data["armonik_images"]["infra"][0]
        modules_version = data["armonik_versions"]["infra"]

    generated_dir_path = repo_path / f"infrastructure/quick-deploy/{environment}/generated"
    generated_dir_path.mkdir(exist_ok=False)

    clone_repo(
        repo_path=generated_dir_path / "infra-modules",
        repo_url=modules_source,
        ref=modules_version,
    )


def main():
    LOG = logging.getLogger(__name__)
    LOG.setLevel(os.environ.get("SETUP_SCRIPT__LOGGING_LEVEL", logging.INFO))

    try:
        repo_path = pathlib.Path(os.environ["SETUP_SCRIPT__REPO_PATH"])
        repo_url = os.environ["SETUP_SCRIPT__REPO_URL"]
        repo_ref = os.environ["SETUP_SCRIPT__REPO_REF"]
        environment = os.environ["SETUP_SCRIPT__AK_ENVIRONMENT"]
        config = json.loads(os.environ["SETUP_SCRIPT__AK_CONFIG"])
        context = os.environ["SETUP_SCRIPT__CONTEXT"]

        if environment not in ["localhost", "gcp"]:
            raise ValueError(f"Deployment environment {environment} not supported.")

    except KeyError as error:
        LOG.error(f"Missing environment variable: {error.args[0]}.")
        sys.exit(1)
    except json.decoder.JSONDecodeError as error:
        LOG.error(
            f"The configuration format is invalid. Error during parsing: {error.msg.lower()}."
        )
        sys.exit(1)
    except ValueError as error:
        LOG.error(error.msg)

    LOG.info(f"Cloning repository {repo_url} in {repo_path}.")
    clone_repo(repo_path=repo_path, repo_url=repo_url, ref=repo_ref)
    LOG.info(f"Editing default parameters file for {environment} deployment.")
    edit_default_parameters_file(repo_path=repo_path, environment=environment, config=config)
    LOG.info("Replacing default Terraform backend.")
    swap_terraform_backend_and_providers(repo_path=repo_path, environment=environment, context=context)
    LOG.info("Downloading ArmoniK's Terraform modules...")
    download_terraform_modules(repo_path=repo_path, environment=environment)

    sys.exit(0)


if __name__ == "__main__":
    main()
