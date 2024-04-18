import shutil

from pathlib import Path

import pytest


@pytest.fixture(scope="function")
def testdir() -> Path:
    p = Path.cwd() / "testdir"
    p.mkdir(exist_ok=False)
    yield p
    shutil.rmtree(p)
