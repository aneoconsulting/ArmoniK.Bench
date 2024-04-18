import json

from pathlib import Path

import pytest

from git.repo import Repo

from armonik_bench.common.git import clone_repo, edit_default_parameters_file


@pytest.fixture
def repo_url() -> str:
    return "https://github.com/aneoconsulting/ArmoniK"


@pytest.mark.parametrize(("ref", "hexsha"), [
    ("ipdrm23", "263b2eeecfa8d8e27544c28c38760e2530fa1f8c"),
    ("392eca93671d8bd9f9bbbddf2b518425401bbc61", "392eca93671d8bd9f9bbbddf2b518425401bbc61"),
    ("v2.19.0", "75e6cc8a80a4d822da2030859d6d56bcb4ccff9d")
])
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
