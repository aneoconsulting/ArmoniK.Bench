import json
import shutil

from pathlib import Path

from git.repo import Repo


def clone_repo(repo_path: Path, repo_url: str, ref: str) -> None:
    """Clone a Git repository from a URL and checkout to a specific commit, branch, or tag.
    If the destination path already exists, it will be overwritten.

    Parameters
    ----------
    repo_path : pathlib.Path
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
    repo = Repo.clone_from(repo_url, repo_path)

    repo.git.checkout(ref)


def edit_default_parameters_file(repo_path: Path, environment: str, config: dict) -> None:
    """Edit the default parameters file for the Terraform ArmoniK deployment.

    This function takes a repository path, an environment name, and a dictionary
    of configuration parameters. It writes the configuration to a Terraform
    parameters file and renames it with a '.json' extension.

    Parameters
    ----------
    repo_path : Path
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

    with parameters_file.open("w") as file:
        file.write(json.dumps(config))

    parameters_file.rename(parameters_file.with_suffix(".tfvars.json"))


def setup_terraform_environment(
    repo_path: Path,
    repo_url: str,
    ref: str,
    environment: str,
    infra_config: dict,
    prefix: str,
    region: str,
) -> dict[str, str]:
    """
    Sets up the Terraform environment by cloning the ArmoniK repository, editing the default
    parameters file, and providing the necessary environment variables.

    Parameters
    ----------
    repo_path : Path
        The path to the local repository where the code will be cloned.
    repo_url : str
        The URL of the ArmoniK repository to clone.
    ref : str
        The reference (branch, tag, or commit) to checkout after cloning.
    environment : str
        Deployment environment ('locahost', 'aws', 'gcp').
    infra_config : dict
        The infrastructure configuration to use for editing the default Terraform parameters file.
    prefix : str
        The prefix of the bucket hosting the Terraform state.
    region : str
        The region where to deploy the infrastructure

    Returns
    -------
    dict[str, str]:
        A dictionary containing the environment variables requied to run the Terraform commands.
            - INGRESS_CERTIFICATES_DIR: The directory for ingress certificates.
            - PARAMETERS_FILE: The path to the parameters file.
            - EXTRA_PARAMETERS_FILE: The path to the extra parameters file.
            - VERSIONS_FILE: The path to the versions file.
            - MODULES_DIR: The directory for infrastructure modules.
            - KUBE_CONFIG_PATH: The path to the Kubernetes configuration file.
            - TF_DATA_DIR: The directory for Terraform data.
            - TF_VAR_namespace: The Terraform variable for namespace.
            - TF_VAR_prefix: The Terraform variable for prefix.
            - REGION: The infrastructure region (using.
            - MODULES_SOURCE: The source of the infrastructure modules.
            - MODULES_VERSION: The version of the infrastructure modules.
            - PREFIX: The prefix of the bucket hosting the Terraform state.

    Raises
    ------
    ValueError
        If the 'repo_path' provided corresponds to an existing file.
    git.exc.GitCommandError
        If an error occurs when cloning the repository or checking out.
    TypeError
        If the 'config' provided is not JSON-serializable.
    """
    clone_repo(repo_path=repo_path, repo_url=repo_url, ref=ref)
    edit_default_parameters_file(repo_path=repo_path, environment=environment, config=infra_config)

    with (repo_path / "versions.tfvars.json").open() as file:
        data = json.loads(file.read())
        modules_source = data["armonik_images"]["infra"][0]
        modules_version = data["armonik_versions"]["infra"]

    generated_dir_path = repo_path / f"infrastructure/quick-deploy/{environment}/generated"
    generated_dir_path.mkdir(exist_ok=False)

    return {
        "INGRESS_CERTIFICATES_DIR": f"{generated_dir_path}/certificates/ingress",
        "PARAMETERS_FILE": "parameters.tfvars.json",
        "EXTRA_PARAMETERS_FILE": "../../../extra.tfvars.json",
        "VERSIONS_FILE": "../../../versions.tfvars.json",
        "MODULES_DIR": f"{generated_dir_path}/infra-modules",
        "KUBE_CONFIG_PATH": "/home/qdelamea/.kube/config",
        "TF_DATA_DIR": str(generated_dir_path),
        "TF_VAR_namespace": "armonik",
        "TF_VAR_prefix": prefix,
        "REGION": region,
        "MODULES_SOURCE": modules_source,
        "MODULES_VERSION": modules_version,
        "PREFIX": prefix,
        "OUTPUT_DIR": f"{generated_dir_path}/armonik-outputs.json",
    }
