import os
import shutil

import pytest


@pytest.fixture(scope="session", autouse=True)
def clean_up():
    yield

    ak_dir = os.path.join(os.getcwd(), "ArmoniK")
    if os.path.exists(ak_dir):
        shutil.rmtree(ak_dir)


@pytest.fixture
def mock_context(mocker):
    # Create a mock context object with necessary attributes/methods
    mock_ctx = mocker.MagicMock()
    # Configure mock_ctx as needed, e.g., mock_ctx.data = ...
    return mock_ctx
