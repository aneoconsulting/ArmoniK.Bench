import pytest


@pytest.fixture
def mock_context(mocker):
    # Create a mock context object with necessary attributes/methods
    mock_ctx = mocker.MagicMock()
    # Configure mock_ctx as needed, e.g., mock_ctx.data = ...
    return mock_ctx
