import json

from pathlib import Path

import pytest

from git.repo import Repo

from armonik_bench.common.git import (
    clone_repo,
    edit_default_parameters_file,
    setup_terraform_environment,
)


@pytest.fixture
def repo_url() -> str:
    return "https://github.com/aneoconsulting/ArmoniK"


@pytest.mark.parametrize(
    ("ref", "hexsha"),
    [
        ("ipdrm23", "263b2eeecfa8d8e27544c28c38760e2530fa1f8c"),
        ("392eca93671d8bd9f9bbbddf2b518425401bbc61", "392eca93671d8bd9f9bbbddf2b518425401bbc61"),
        ("v2.19.0", "75e6cc8a80a4d822da2030859d6d56bcb4ccff9d"),
    ],
)
def test_clone_repo(testdir: Path, repo_url: str, ref: str, hexsha: str) -> None:
    clone_repo(testdir, repo_url, ref)
    repo = Repo(testdir)
    assert repo.head.object.hexsha == hexsha


def test_edit_default_parameters_file(testdir: Path, repo_url: str) -> None:
    config = {"test": "ok"}
    clone_repo(testdir, repo_url, "main")
    edit_default_parameters_file(testdir, "localhost", config)

    parameters_file = testdir / "infrastructure/quick-deploy/localhost/parameters.tfvars"

    assert not parameters_file.exists()

    with parameters_file.with_suffix(".tfvars.json").open() as file:
        assert json.loads(file.read()) == config


def test_setup_terraform_environment(testdir: Path, repo_url: str) -> None:
    env_vars = setup_terraform_environment(
        repo_path=testdir,
        repo_url=repo_url,
        ref="3339a4e0f33bd0629b5d6b99d23e2e72ee6af49a",
        environment="localhost",
        infra_config={"test": "ok"},
        prefix="prefix",
        region="region",
    )
    generated_path = testdir / "infrastructure/quick-deploy/localhost/generated"
    assert env_vars["INGRESS_CERTIFICATES_DIR"] == str(generated_path / "certificates/ingress")
    assert env_vars["PARAMETERS_FILE"] == "parameters.tfvars.json"
    assert env_vars["EXTRA_PARAMETERS_FILE"] == "../../../extra.tfvars.json"
    assert env_vars["VERSIONS_FILE"] == "../../../versions.tfvars.json"
    assert env_vars["MODULES_DIR"] == str(generated_path / "infra-modules")
    assert env_vars["KUBE_CONFIG_PATH"] == "$(HOME)/.kube/config"
    assert env_vars["TF_DATA_DIR"] == str(generated_path)
    assert env_vars["TF_VAR_namespace"] == "armonik"
    assert env_vars["TF_VAR_prefix"] == "prefix"
    assert env_vars["REGION"] == "region"
    assert env_vars["MODULES_SOURCE"] == "https://github.com/aneoconsulting/ArmoniK.Infra.git"
    assert env_vars["MODULES_VERSION"] == "0.3.1-pre-1-9ac6142"
    assert env_vars["PREFIX"] == "prefix"
