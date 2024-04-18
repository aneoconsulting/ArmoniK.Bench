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
    FileNotFoundError
        If the 'repo_path' doesn't correspond to a local clone of the ArmoniK repository.
    """
    parameters_file = repo_path / f"infrastructure/quick-deploy/{environment}/parameters.tfvars"

    with parameters_file.open("w") as file:
        file.write(json.dumps(config))

    parameters_file.rename(parameters_file.with_suffix(".tfvars.json"))
