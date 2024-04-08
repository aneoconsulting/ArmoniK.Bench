import sys

from pathlib import Path


def pytest_configure(config):
    """
    Fix Python import paths before test collection begins.
    """
    sys.path.append(str(Path.cwd()))
